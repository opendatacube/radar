"""
Prepares individual scenes (specifically an orthorectified Sentinel 1 scene in BEAM-DIMAP format) for datacube indexing.
The BEAM-DIMAP format (output by Sentinel Toolbox/SNAP) consists of an XML header file (.dim)
and a directory (.data) which stores different polarisations (different raster bands) separately,
each as ENVI format, that is, raw binary (.img) with ascii header (.hdr). 
Each scene needs to have a _yaml.info file (created during the ARD processing) for creating the yaml file. 
The output yaml file captures the information required to meet the CARD4L specifications where possible.
Adapted from code written by Ben Lewis and Fang Yuan.

September 2019 - Updated to look for  _yaml.info file, and it not found it uses the old method of looking for the
 .list and .out files to extract relevant metadata
"""

import rasterio
import rasterio.features
import shapely.affinity
import shapely.geometry
import shapely.ops
from osgeo import osr
from xml.dom import minidom

# get corner coords in crs of source datafile,
# transform into crs of datacube index.
def get_geometry(path):
    with rasterio.open(path) as img:
        left, bottom, right, top = img.bounds
        crs = str(str(getattr(img, 'crs_wkt', None) or img.crs.wkt))
        corners = {
                    'ul': {'x': left, 'y': top},
                    'ur': {'x': right, 'y': top},
                    'll': {'x': left, 'y': bottom},
                    'lr': {'x': right, 'y': bottom} 
                  }
        projection = {'spatial_reference': crs, 'geo_ref_points': corners}

        spatial_ref = osr.SpatialReference(crs)
        t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())
        def transform(p):
            lon, lat, z = t.TransformPoint(p['x'], p['y'])
            return {'lon': lon, 'lat': lat}
        extent = {key: transform(p) for key,p in corners.items()}

        return projection, extent


# IMAGE BOUNDARY CODE to extract correct image extent
def safe_valid_region(images, mask_value=None):
    try:
        return valid_region(images, mask_value)
    except (OSError, RasterioIOError):
        return None


def valid_region(images, mask_value=None):
    mask = None

    for fname in images:
        # ensure formats match
        with rasterio.open(str(fname), 'r') as ds:
            transform = ds.transform
            img = ds.read(1)

            if mask_value is not None:
                new_mask = img & mask_value == mask_value
            else:
                new_mask = img != 0 #ds.nodata
            if mask is None:
                mask = new_mask
            else:
                mask |= new_mask

    shapes = rasterio.features.shapes(mask.astype('uint8'), mask=mask)
    shape = shapely.ops.unary_union([shapely.geometry.shape(shape) for shape, val in shapes if val == 1])

    # convex hull
    geom = shape.convex_hull

    # buffer by 1 pixel
    geom = geom.buffer(1, join_style=3, cap_style=3)

    # simplify with 1 pixel radius
    geom = geom.simplify(1)

    # intersect with image bounding box
    geom = geom.intersection(shapely.geometry.box(0, 0, mask.shape[1], mask.shape[0]))

    # transform from pixel space into CRS space
    geom = shapely.affinity.affine_transform(geom, (transform.a, transform.b, transform.d,
                                                    transform.e, transform.xoff, transform.yoff))

    output = shapely.geometry.mapping(geom)
    output['coordinates'] = _to_lists(output['coordinates'])
    return output

def _to_lists(x):
    #Returns lists of lists when given tuples of tuples
    if isinstance(x, tuple):
        return [_to_lists(el) for el in x]

    return x
# END IMAGE BOUNDARY CODE

import uuid
from xml.etree import ElementTree
from dateutil import parser
import os

# Construct metadata dict
def prep_dataset(path):
    # input: path = .dim filename

    # Read in the XML header and extract relevant metadata
    xml = ElementTree.parse(str(path)).getroot().find("Dataset_Sources/MDElem[@name='metadata']/MDElem[@name='Abstracted_Metadata']")
    scene_name = xml.find("MDATTR[@name='PRODUCT']").text
    platform = xml.find("MDATTR[@name='MISSION']").text.replace('-','_')
    t0 = parser.parse(xml.find("MDATTR[@name='first_line_time']").text) # start time
    t1 = parser.parse(xml.find("MDATTR[@name='last_line_time']").text) # end time
    IN = xml.find("MDATTR[@name='incidence_near']").text # Incidence angle - near
    IF = xml.find("MDATTR[@name='incidence_far']").text # Incidence angle - far
    AL = xml.find("MDATTR[@name='azimuth_looks']").text # Azimuth looks
    RL = xml.find("MDATTR[@name='range_looks']").text # Range looks
    AS = xml.find("MDATTR[@name='azimuth_spacing']").text # Azumith spacing
    RS = xml.find("MDATTR[@name='range_spacing']").text # Range spacing
    SN = xml.find("MDATTR[@name='slice_num']").text # Slice number
    DEM = xml.find("MDATTR[@name='DEM']").text # DEM (currently not used since it is the temporary file
    geotransform = ElementTree.parse(str(path)).getroot().find("Geoposition/IMAGE_TO_MODEL_TRANSFORM").text
    height = ElementTree.parse(str(path)).getroot().find("Raster_Dimensions/NROWS").text
    width = ElementTree.parse(str(path)).getroot().find("Raster_Dimensions/NCOLS").text

    # get band names, datatype and no data value (assume bands = ['alpha','anisotropy','entropy'])
    Num_Bands = ElementTree.parse(str(path)).getroot().find("Raster_Dimensions/NBANDS").text
    bandpathnames = ['' for x in range(int(Num_Bands))]
    bands = ['' for x in range(int(Num_Bands))]

    for NB in range(int(Num_Bands)):
        bandpathnames[NB] =minidom.parse(path).getElementsByTagName('DATA_FILE_PATH')[NB].attributes['href'].value.replace('hdr','img')
        if bandpathnames[NB].endswith('Alpha.img'): bands[NB] = 'alpha'
        if bandpathnames[NB].endswith('Anisotropy.img'): bands[NB] = 'anisotropy'
        if bandpathnames[NB].endswith('Entropy.img'): bands[NB] = 'entropy'
    
    DataType=[]
    DataType += [spectral_band_info.find('DATA_TYPE').text for spectral_band_info in ElementTree.parse(str(path)).getroot().findall('Image_Interpretation/Spectral_Band_Info')]

    NoDataVal=[]
    NoDataVal += [spectral_band_info.find('NO_DATA_VALUE').text for spectral_band_info in ElementTree.parse(str(path)).getroot().findall('Image_Interpretation/Spectral_Band_Info')]

    # Define details to extract from the yaml.info and S1 level-1 input files
    processing_output_file, processing_message, time_processed, input_xml_file = 'not found', 'not found', 'unknown', 'not found'
    job_id, SARA_id = 'processed on VDI', 'not_found'
    SatName, Instrument, Identifier = 'not found', 'not found', 'not found'
    Mode, Size, S1extent = 'not found', 'not found', 'not found' 
    Abs_Orbit, Rel_Orbit, Pass_Dir, Pol_Values = 'not found', 'not found', 'not found', 'not found'
    S1T0, S1T1, Level, Type = 'not found', 'not found', 'not found', 'not found'
    SNAP_GPF, SNAP_SAR, SNAP_Cal, SNAP_Tools, SNAP_Pol = 'not found', 'not found', 'not found', 'not found', 'not found'
    
    # Read the _yaml.info file to extract metadata details
    exists = os.path.isfile(path.replace('.dim','_yaml.info'))
    if exists:
        yaml_info_file = path.replace('.dim','_yaml.info')
        f = open(yaml_info_file)
        for L in f:
            if L.find('proc log file:') != -1: processing_output_file = L.split(': ')[1].strip()
            if L.find('scene processed:') != -1: processing_message = L.split(': ')[1].strip()
            if L.find('proc date:') != -1: time_processed = L.split(': ')[1].strip()
            if L.find('NCI job ID:') != -1: job_id = L.split(': ')[1].strip()
            if L.find('input SARA ID:') != -1: SARA_id = L.split(': ')[1].strip()
            if L.find('SNAP Graph Processing Framework (GPF):') != -1: SNAP_GPF = L.split(': ')[1].strip()
            if L.find('S1TBX SAR Processing:') != -1: SNAP_SAR = L.split(': ')[1].strip()
            if L.find('S1TBX Calibration:') != -1: SNAP_Cal = L.split(': ')[1].strip()
            if L.find('S1TBX Sentinel-1 Tools:') != -1: SNAP_Tools = L.split(': ')[1].strip()
            if L.find('RSTB Polarimetric SAR Tools:') != -1: SNAP_Pol = L.split(': ')[1].strip()
            if L.find('input xml:') != -1: input_xml_file = L.split(': ')[1].strip()       
        f.close
    else:    
        print('No _yaml.info file found for scene. Looking through .out file instead!')

        text_sub = "Zip file nr."
        statis_line = "temporary line"
        for root, dirs, jobfiles in os.walk(ARD_output_log_directory):
            for jobfile in jobfiles:
                if jobfile.endswith('.list'):
                    jobfilename=os.path.join(root,jobfile)
                    f = open(jobfilename, "r") # open the .list file to look for scene name
                    for L in f:
                        if L.find(scene_name) != -1:
                            try: SARA_id = L.split(' ')[1]
                            except: print('SARA_id not found')
                            exists = os.path.isfile(jobfilename.replace('.list','.out'))
                            if exists:
                                processing_output_file = jobfilename.replace('.list','.out')
                                f2 = open(processing_output_file, "r") # scene name found, so opening .out file
                                for L2 in f2:
                                    if L2.find(text_sub) != -1: temp_file_nr = L2.split(' ')[3]
                                    if L2.find(scene_name) != -1:
                                        file_nr = temp_file_nr.strip()
                                        if os.path.isfile(L2.strip().replace('.zip','.xml')): input_xml_file = L2.strip().replace('.zip','.xml')
                                        statis_line = "Scene processed ("+file_nr
                                    if L2.find(statis_line) != -1: processing_message = L2.rstrip().split(':')[1] 
                                    if L2.find("Resource Usage on") != -1: time_processed = L2.lstrip().split(' ')[3]+' '+L2.split(' ')[4].rstrip()
                                    if L2.find("Job Id:") != -1: job_id = L2.split(' ')[len(L2.split(' '))-1].rstrip()
                                f2.close                                      
                    f.close

    # Read input scene .xml file to extract remaining metadata details
    if input_xml_file != 'not found':
        xml_ImageDoc = minidom.parse(input_xml_file)
        satellite = xml_ImageDoc.getElementsByTagName('SATELLITE')
        SatName =xml_ImageDoc.getElementsByTagName('SATELLITE')[0].attributes['name'].value
        S1T0 = xml_ImageDoc.getElementsByTagName('ACQUISITION_TIME')[0].attributes['start_datetime_utc'].value
        S1T1 = xml_ImageDoc.getElementsByTagName('ACQUISITION_TIME')[0].attributes['stop_datetime_utc'].value
        Instrument = ElementTree.parse(str(input_xml_file)).getroot().find("INSTRUMENT").text
        Identifier = ElementTree.parse(str(input_xml_file)).getroot().find("IDENTIFIER").text
        Mode =xml_ImageDoc.getElementsByTagName('MODE')[0].attributes['value'].value
        Size =xml_ImageDoc.getElementsByTagName('ZIPFILE')[0].attributes['size_bytes'].value
        S1extent = ElementTree.parse(str(input_xml_file)).getroot().find('ESA_TILEOUTLINE_FOOTPRINT_WKT').text.rstrip()
        Abs_Orbit =xml_ImageDoc.getElementsByTagName('ORBIT_NUMBERS')[0].attributes['absolute'].value
        Rel_Orbit =xml_ImageDoc.getElementsByTagName('ORBIT_NUMBERS')[0].attributes['relative'].value
        Pass_Dir =xml_ImageDoc.getElementsByTagName('PASS')[0].attributes['direction'].value
        Pol_Values =xml_ImageDoc.getElementsByTagName('POLARISATION')[0].attributes['values'].value
        Level = ElementTree.parse(str(input_xml_file)).getroot().find('PROCESSING_LEVEL').text
        Type = ElementTree.parse(str(input_xml_file)).getroot().find('PRODUCT_TYPE').text
    else: print('no input scene xml file found')

    # Need to define where the relative directory begins for image bands (it is currently 4th folder from final image i.e. path_split[5:])
    rel_dir_num = 4
    path_split = path.split('/')
    for n in range(1,len(path_split)): path_split[n]='/'+path_split[n]
    rel_path_dim = ''.join([s for s in path_split[rel_dir_num+1:] if isinstance(s,str)])
    bandpaths = [str(os.path.join(path[:-3]+'data/',bandpathnames[NB].split('/')[1])) for NB in range(int(Num_Bands))]
    rel_bandpaths = [str(os.path.join(rel_path_dim[:-3]+'data/',bandpathnames[NB].split('/')[1])) for NB in range(int(Num_Bands))]

    # Need to define where the relative directory begins for processing output file
    rel_pof = 'not found'
    if processing_output_file != 'not found':
        pof_split = processing_output_file.split('/')
        if len(pof_split)<rel_dir_num+1: rel_pof = processing_output_file
        else: 
            for n in range(1,len(pof_split)): pof_split[n]='/'+pof_split[n]
            rel_pof = ''.join([s for s in pof_split[rel_dir_num+1:] if isinstance(s,str)])

    # get appropriate filenames
    safe_name = scene_name + '.SAFE'
    if SARA_id == 'not found': 
        product_url = 'https://copernicus.nci.org.au/sara.client/#/home' 
    else:
        product_url = 'https://copernicus.nci.org.au/sara.client/#/collections/S1/' + SARA_id.strip()

    # Extract spatial resolution mode and define ENL (assume that it is IW mode ONLY)
    ResAndType = scene_name.split('_')[2]
    Res = ResAndType[len(ResAndType)-1:]
    Resolution = ' unknown '
    ENL = ' unknown '
    if Res == 'H': Resolution, ENL = 'High', '4.4'
    if Res == 'M': Resolution, ENL = 'Medium', '81.8'    
    if ResAndType == 'SLC': ENL = '1.0'
    if ResAndType == 'SLC': Resolution = '(SLC)'

    # check that the start and stop times are the same from source and derived scene
    S1T0 = t0 if str(t0) == str(S1T0) else print('Warning: t0 (',str(t0),') not equal to S1T0 (',str(S1T0),')')  
    S1T1 = t1 if str(t1) == str(S1T1) else print('Warning: t1 (',str(t1),') not equal to S1T1 (',str(S1T1),')')
    centre_time = str(t0+(t1-t0)/2)
       
    # trusting bands coaligned, use one to generate spatial bounds for all
    projection, extent = get_geometry(bandpaths[0])    

    # valid data boundary for all 
    projection['valid_data'] = safe_valid_region(bandpaths)

    # format metadata (i.e. construct hashtable tree for syntax of file interface
    return {
        'platform': {'code': 'SENTINEL_1'},
        'instrument': {'name': 'SAR_C'},
        'processing_level': "terrain",
        'product_type': "dual_pol_decomp",
        'id': str(uuid.uuid4()),
        'extent': { 'coord': extent, 'from_dt': str(t0), 'to_dt': str(t1), 'center_dt': str(t0+(t1-t0)/2) },
        'format': {'name': 'ENVI'},
        'grid_spatial': {'spatial_projection': projection['spatial_reference'], 'polygon_extent': str(projection['valid_data']['coordinates'])},
        'image': { 'bands': {b: {'path': p, 'nodata': 0, 'geotransform': geotransform, 'image_height': ast.literal_eval(height), 'image_width': ast.literal_eval(width)} for b,p in set(zip(bands,rel_bandpaths))} },
        'metadata_information': {'metadata_filename': rel_path_dim, 'incidence_angle_near': ast.literal_eval(IN), 'incidence_angle_far': ast.literal_eval(IF), 'spacing_azimuth': ast.literal_eval(AS), 'spacing_range': ast.literal_eval(RS), 'dem_used': 'SRTM 1sec HGT'},
        'pixel_origin': 'UL (0.5, 0.5)', 
        'processing_output_message': processing_message,
        'processing_output_file': rel_pof,
        'geometric_accuracy':'unknown',
        'lineage': { 'source_datasets': {'sentinel_1': {'mission_id': SatName, 'identifier': Identifier, 'product_url': product_url, 'instrument': Instrument, 'mode': Mode, 'size_(bytes)': ast.literal_eval(Size), 'slice_number': ast.literal_eval(SN), 'format': 'SAFE', 'extent': { 'coord': S1extent.strip('\n'), 'from_dt': str(S1T0), 'to_dt': str(S1T1), 'center_dt': str(centre_time)},  'orbit_number': ast.literal_eval(Abs_Orbit), 'relative_orbit': ast.literal_eval(Rel_Orbit), 'pass_direction': Pass_Dir, 'polarisation': Pol_Values, 'product_level': Level, 'product_type': Type, 'resolution': Resolution, 'looks_azimuth': ast.literal_eval(AL), 'looks_range': ast.literal_eval(RL), 'calibration_constant_dB': ast.literal_eval('0'), 'look_direction': 'right', 'centre_frequency_GHz': ast.literal_eval('5.405'), 'equivalent_number_looks': ast.literal_eval(ENL),
        'noise_equivalent_sigma_zero': '-22dB', 'sensor_calibration_manuscripts': '"Radiometric accuracy and stability of sentinel-1A determined using point targets" (https://doi.org/10.1017/S1759078718000016), "Radiometric accuracy and stability of sentinel-1A determined using point targets" (https://doi.org/10.1017/S1759078718000016), "GMES Sentinel-1 mission" (https://doi.org/10.1016/j.rse.2011.05.028)'} } },
        'software_versions': {'SAR_ARD_code': {'repo_url': 'https://github.com/opendatacube/radar', 'version': '1.0.0'}, 'SNAP': {'repo_url': 'https://step.esa.int/main/download/', 'SNAP Graph Processing Framework version': SNAP_GPF, 'S1TBX SAR Processing version': SNAP_SAR, 'S1TBX Calibration version': SNAP_Cal, 'S1TBX Sentinel-1 Tools version': SNAP_Tools, 'RSTB Polarimetric SAR Tools version': SNAP_Pol} },
        'system_information': {'time_processed': time_processed, 'job_id': job_id}
        
    }


import sys
import yaml
from collections import OrderedDict
import ast

def represent_dictionary_order(self, dict_data):
    return self.represent_mapping('tag:yaml.org,2002:map', dict_data.items())

def setup_yaml():
    yaml.add_representer(OrderedDict, represent_dictionary_order)

setup_yaml()

# Define ARD_output_log_directory where the .list and .out log processing files exist
ARD_output_log_directory = '/g/data/qd04/Cate/log_dual_pol_test/'
ARD_scene_directory = '/g/data/qd04/Cate/Sentinel-1/C-SAR/SLC/'
for root, dirs, files in os.walk(ARD_scene_directory):
    for file in files:
        if file.endswith('.dim'):
            scene=os.path.join(root,file)
            yaml_path = scene.replace('.dim','.yaml')
            print('yaml_path =',yaml_path)
            if not os.path.exists(yaml_path):
                try:
                    metadata = prep_dataset(scene)
                    print("scene =",scene)
                    #For changing output order to that defined in the dic (otherwise it is alphabetical)
                    dic = OrderedDict(metadata)
                    with open(yaml_path,'w') as stream:
                        yaml.dump(dic, stream, default_flow_style=False)
                except:
                    print("Error preping dataset:", scene)

