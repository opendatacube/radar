"""
Prepares individual scenes (specifically an orthorectified Sentinel 1 scene in BEAM-DIMAP format) for datacube indexing.
The BEAM-DIMAP format (output by Sentinel Toolbox/SNAP) consists of an XML header file (.dim)
and a directory (.data) which stores different polarisations (different raster bands) separately,
each as ENVI format, that is, raw binary (.img) with ascii header (.hdr). 
Each scene needs to have a _yaml.info file (created during the ARD processing) for creating the yaml file. 
The output yaml file captures the information required to meet the CARD4L specifications where possible.

Written by Ben Lewis, Cate Ticehurst and Fang Yuan.

"""

import rasterio
import rasterio.features
import shapely.affinity
import shapely.geometry
import shapely.ops
from osgeo import osr
from xml.dom import minidom
from skimage.morphology import binary_dilation, binary_erosion, disk, square
from multiprocessing.pool import ThreadPool as Pool

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
    except (OSError, rasterio.RasterioIOError):
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

    # fill holes
    #mask = binary_erosion(binary_dilation(mask, square(20)), square(20))

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
    if 'coordinates' in output:
        output['coordinates'] = _to_lists(output['coordinates'])
        return output
    else:
        return None

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

    # Read in the ARD scene XML header (i.e. .dim file) and extract relevant metadata
    xml = ElementTree.parse(str(path)).getroot().find("Dataset_Sources/MDElem[@name='metadata']/MDElem[@name='Abstracted_Metadata']")
    scene_name = xml.find("MDATTR[@name='PRODUCT']").text
    mission_id = 'S'+xml.find("MDATTR[@name='MISSION']").text.split('-')[1]
    product_type = xml.find("MDATTR[@name='PRODUCT_TYPE']").text
    product_level = "Level-1"
    mode = xml.find("MDATTR[@name='ACQUISITION_MODE']").text
    abs_orbit = xml.find("MDATTR[@name='ABS_ORBIT']").text
    rel_orbit = xml.find("MDATTR[@name='REL_ORBIT']").text
    pass_dir = xml.find("MDATTR[@name='PASS']").text.title()
    pols = [xml.find("MDATTR[@name='mds%d_tx_rx_polar']"%i).text for i in range(1,5)]
    pol_values = ','.join([p for p in pols if p !='-'])
    t0 = parser.parse(xml.find("MDATTR[@name='first_line_time']").text) # start time
    t1 = parser.parse(xml.find("MDATTR[@name='last_line_time']").text) # end time
    IN = xml.find("MDATTR[@name='incidence_near']").text # Incidence angle - near
    IF = xml.find("MDATTR[@name='incidence_far']").text # Incidence angle - far
    AL = xml.find("MDATTR[@name='azimuth_looks']").text # Azimuth looks
    RL = xml.find("MDATTR[@name='range_looks']").text # Range looks
    AS = xml.find("MDATTR[@name='azimuth_spacing']").text # Azumith spacing
    RS = xml.find("MDATTR[@name='range_spacing']").text # Range spacing
    SN = xml.find("MDATTR[@name='slice_num']").text # Slice number
    DEM = xml.find("MDATTR[@name='DEM']").text # DEM used
    geotransform = ElementTree.parse(str(path)).getroot().find("Geoposition/IMAGE_TO_MODEL_TRANSFORM").text
    height = ElementTree.parse(str(path)).getroot().find("Raster_Dimensions/NROWS").text
    width = ElementTree.parse(str(path)).getroot().find("Raster_Dimensions/NCOLS").text

    try:
        footprint = ElementTree.parse(str(path)).getroot().find("Dataset_Sources/MDElem[@name='metadata']/MDElem[@name='Original_Product_Metadata']/MDElem[@name='XFDU']/MDElem[@name='metadataSection']/MDElem[@name='metadataObject']/MDElem[@name='metadataWrap']/MDElem[@name='xmlData']/MDElem[@name='frameSet']/MDElem[@name='frame']/MDElem[@name='footPrint']")
        S1extent = footprint.find("MDATTR[@name='coordinates']").text # input scene extent
    except:
        S1extent = "not found"
 
    # get band names, datatype and no data value (assume bands = ['vh','vv','local_incidence_angle'])
    Num_Bands = ElementTree.parse(str(path)).getroot().find("Raster_Dimensions/NBANDS").text
    bandpathnames = ['' for x in range(int(Num_Bands))]
    bands = ['' for x in range(int(Num_Bands))]

    for NB in range(int(Num_Bands)):
        bandpathnames[NB] = os.path.join(os.path.dirname(path),minidom.parse(path).getElementsByTagName('DATA_FILE_PATH')[NB].attributes['href'].value.replace('hdr','img'))
        if bandpathnames[NB].endswith('Gamma0_VH.img'): bands[NB] = 'vh'
        if bandpathnames[NB].endswith('Gamma0_VV.img'): bands[NB] = 'vv'
        if bandpathnames[NB].endswith('localIncidenceAngle.img'): bands[NB] = 'lia'
    
    DataType=[]
    DataType += [spectral_band_info.find('DATA_TYPE').text for spectral_band_info in ElementTree.parse(str(path)).getroot().findall('Image_Interpretation/Spectral_Band_Info')]

    NoDataVal=[]
    NoDataVal += [spectral_band_info.find('NO_DATA_VALUE').text for spectral_band_info in ElementTree.parse(str(path)).getroot().findall('Image_Interpretation/Spectral_Band_Info')]

    # Define details to extract from the yaml.info and S1 level-1 input files
    processing_output_file, processing_message, time_processed, input_xml_file = 'not found', 'not found', 'unknown', 'not found'
    job_id, SARA_id = 'processed on VDI or unknown', 'not_found'
    SNAP_GPF, SNAP_SAR, SNAP_Cal = 'not found', 'not found', 'not found'

    # Read the _yaml.info file to extract metadata details, or if it doesnt exists, read through the .list and .out log files
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
            if L.find('input xml:') != -1: input_xml_file = L.split(': ')[1].strip()       
        f.close

    # get appropriate filenames
    safe_name = scene_name + '.SAFE'
    if SARA_id == 'not_found': 
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

    centre_time = str(t0+(t1-t0)/2)
       
    # trusting bands coaligned, use one to generate spatial bounds for all
    projection, extent = get_geometry(bandpathnames[0])    

    # valid data boundary for all 
    projection['valid_data'] = safe_valid_region(bandpathnames)

    # format metadata (i.e. construct hashtable tree for syntax of file interface
    return {
        'platform': {'code': 'SENTINEL_1'},
        'instrument': {'name': 'SAR_C'},
        'processing_level': "terrain",
        'product_type': "gamma0",
        'id': str(uuid.uuid4()),
        'extent': { 'coord': extent, 'from_dt': str(t0), 'to_dt': str(t1), 'center_dt': str(t0+(t1-t0)/2) },
        'format': {'name': 'ENVI'},
        'grid_spatial': {'projection': projection},
        'image': { 'bands': {b: {'path': p, 'nodata': 0, 'geotransform': geotransform, 'height': ast.literal_eval(height), 'width': ast.literal_eval(width)} for b,p in set(zip(bands,bandpathnames))} },
        'metadata_information': {'metadata_filename': path, 'mission_id': mission_id,
                                 'orbit_number': abs_orbit, 'relative_orbit': '%03d'%int(rel_orbit), 
                                 'pass_direction': pass_dir, 'polarisation': pol_values, 
                                 'slice_number': '%03d'%int(SN),
                                 'incidence_angle_near': ast.literal_eval(IN), 'incidence_angle_far': ast.literal_eval(IF), 'spacing_azimuth': ast.literal_eval(AS), 'spacing_range': ast.literal_eval(RS), 'dem_used': DEM},
        'pixel_origin': 'UL (0.5, 0.5)', 
        'processing_output_message': processing_message,
        'processing_output_file': processing_output_file,
        'geometric_accuracy':'unknown',
        'lineage': { 'source_datasets': {'sentinel_1': {'identifier': scene_name, 'product_url': product_url, 'instrument': 'SAR_C', 'mode': mode, 'format': 'SAFE', 'extent': { 'coord': S1extent, 'from_dt': str(t0), 'to_dt': str(t1), 'center_dt': str(centre_time)}, 'product_level': product_level, 'product_type': product_type, 'resolution': Resolution, 'looks_azimuth': ast.literal_eval(AL), 'looks_range': ast.literal_eval(RL), 'calibration_constant_dB': ast.literal_eval('0'), 'look_direction': 'right', 'centre_frequency_GHz': ast.literal_eval('5.405'), 'equivalent_number_looks': ast.literal_eval(ENL),
        'noise_equivalent_sigma_zero': '-22dB', 'sensor_calibration_manuscripts': '"Radiometric accuracy and stability of sentinel-1A determined using point targets" (https://doi.org/10.1017/S1759078718000016), "Radiometric accuracy and stability of sentinel-1A determined using point targets" (https://doi.org/10.1017/S1759078718000016), "GMES Sentinel-1 mission" (https://doi.org/10.1016/j.rse.2011.05.028)'} } },
        'software_versions': {'SAR_ARD_code': {'repo_url': 'https://github.com/opendatacube/radar', 'version': '1.0.0'}, 'SNAP': {'repo_url': 'https://step.esa.int/main/download/', 'SNAP Graph Processing Framework version': SNAP_GPF, 'S1TBX SAR Processing version': SNAP_SAR, 'S1TBX Calibration version': SNAP_Cal} },
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

if len(sys.argv)>1:
    scene = sys.argv[1]
    yaml_path = os.path.basename(scene).replace('.dim','.yaml')
    
    metadata = prep_dataset(scene)
    print("scene =",scene)
    #For changing output order to that defined in the dic (otherwise it is alphabetical)
    dic = OrderedDict(metadata)
    with open(yaml_path,'w') as stream:
        yaml.dump(dic, stream, default_flow_style=False)

    exit()


import glob

scenes = glob.glob('/g/data/dz56/backscatter/Sentinel-1/C-SAR/GRD/**/*.dim',recursive=True)                             
outputdir = 'backscatter_yamls_v11'
if not os.path.exists(outputdir): os.mkdir(outputdir)

def make_yaml(scene, outputdir=outputdir):
    yaml_path = os.path.join(outputdir, os.path.basename(scene).replace('.dim','.yaml'))
    if not os.path.exists(yaml_path):
        try:
            metadata = prep_dataset(scene)
            #For changing output order to that defined in the dic (otherwise it is alphabetical)
            dic = OrderedDict(metadata)
            with open(yaml_path,'w') as stream:
                yaml.dump(dic, stream, default_flow_style=False)
            print("scene =", scene)
            return yaml_path
        except:
            print("Error preping dataset:", scene)
            return None
    else: 
        return None

pool = Pool(3)
for yaml_path in pool.imap_unordered(make_yaml, scenes):
    if yaml_path:
       command = 'datacube -C radar.conf dataset add -p s1_gamma0_scene_v11 --confirm-ignore-lineage ' + yaml_path
       os.system(command)
            
pool.close()
pool.join()
