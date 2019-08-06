#!/usr/bin/env python

## Creates a file of DEM data. The extents of the output file are determined either 
## from the extents of a scene of Sentinel-1 data, or from a user-selected bounding 
## box. Also, the output DEM data is generated either as a subset of a DEM file 
## dataset, or as a mosaic of (pre-downloaded) DEM tiles. 
## The code executes 'gdalwarp' or 'gdal_translate' as an operating system command 
## (i.e. use 'module load gdal' prior to executing this python script on NCI).
## 
## The list of possible input parameters (with typical examples) is provided below.
## All parameters are optional, unless otherwise specified.
##
##  --scene_file /path/to/SAR_scene_file.zip
##     Path to a .zip file of SAR scene data, which will be used to determine the
##     extents of the generated DEM data. This argument can be defined as a list of 
##     comma-separated file names (no spaces) defining the names of multiple .zip 
##     files to use in order to determine (global) extents. Alternatively, use --bbox 
##     instead (one of the two parameters must be used). 
##  --bbox 130.0 131.0 -21.0 -20.0
##     Lat/long bounding box determining the extent of the generated DEM data.
##     Alternatively, use --scene_file (one of the two must be used).
##  --DEM_source /path/to/DEM_data
##     Sets the source of DEM data to use, either a file (e.g. GTiff) or a folder of 
##     DEM tiles. If set to a file, the output DEM data will be subsetted from it. If 
##     set to a folder, the tiles' DEM data will be composited into a mosaic. Default 
##     is "/g/data1a/qd04/SNAP_DEM_data" (DEF_SOURCE_DIR in the code below).
##  --output_file /path/to/DEM_output.tif
##     Path to the output file of DEM data. Non-optional parameter.
##
## Examples:
##  > module load python3/3.4.3 python3/3.4.3-matplotlib gdal
##  > python3.4 make_DEM.py --bbox 145.75 150.75 -37.25 -34.5 --output_file ./ZSZ_DEM/DEM.tif
##  > python3.4 make_DEM.py --bbox 142.3 143.3 -35.7 -34.7 --output_file ./DEM_subset.tif
##  > python3.4 make_DEM.py --scene_file scene1.zip,scene2.zip --DEM_source sourceDEM.tif --output_file ./DEM_subset.tif

## TODO: . general handling of potential issues, check for file names, etc.


import argparse
import numpy as np
import sys, os

DEF_SOURCE_DIR = "/g/data1a/qd04/SNAP_DEM_data"


def main():
    # input parameters:
    parser = argparse.ArgumentParser(description="Generate DEM data as a subset or a mosaic.")
    parser.add_argument("--scene_file", default=None, 
                        help="Input ZIP file of Sentinel-1 scene to determine the extents of the output DEM data.")
    parser.add_argument("--DEM_source", default=DEF_SOURCE_DIR, 
                        help="Source of DEM data, either a file (e.g. GTiff) or a folder of DEM tiles. Default is %(default)s.")
    parser.add_argument("--output_file", default=None, help="Created output file of DEM data.")
    parser.add_argument("--bbox", nargs=4, type=float, default=None, metavar=('westLong', 'eastLong', 'southLat', 'northLat'),
                        help=("Lat/long bounding box for extents of the output DEM data."))
    cmdargs = parser.parse_args()       # parse options
    
    if cmdargs.scene_file is not None and cmdargs.bbox is not None:
        sys.exit("Error: both 'scene_file' and 'bbox' are defined (only one needed to determine extents).")
    if cmdargs.output_file is None:
        sys.exit("Error: name of output DEM file is not defined.")
    
    tmp = os.path.dirname(cmdargs.output_file)
    if tmp!='' and not os.path.isdir( tmp ):
        os.mkdir(tmp)
        
    
    #=== Get the desired DEM output extents:
    if cmdargs.bbox is not None:      # user-selected extents
        (lon_min, lon_max, lat_min, lat_max) = cmdargs.bbox
        
    elif cmdargs.scene_file is not None:      # get extents from scene's / scenes' .xml file
        longs = np.array([])
        lats = np.array([])
        
        for zipfile in cmdargs.scene_file.split(','):   # process multiple scenes if needed
            xml_file = zipfile.replace('.zip','.xml')
            ## # Use this if we can rely on the .xml file's named elements:
            ## import xml.etree.ElementTree as ET
            ## tree = ET.parse(xml_file)
            ## root = tree.getroot()
            ## root.find('ESA_TILEOUTLINE_FOOTPRINT_WKT').text
            ## #'\n    POLYGON ((146.01001 -43.953518,143.035568 -43.23727,143.645248 -41.895309,146.561462 -42.597675,146.01001 -43.953518))\n  '

            # Otherwise simply find 'POLYGON' string in .xml file:
            with open (xml_file, 'rt') as infile:
                for line in infile:
                    if line.find('POLYGON')!=-1: break  # find line with 'POLYGON' in it

            for cc in ['POLYGON','(',')','\n']:     # remove unwanted characters
                line = line.replace(cc,'')

            line = line.replace(',',' ').split()
            line = np.array( list(map(lambda x: float(x), line)) )   # convert to array of numeric lat/lon coords
            
            longs = np.concatenate( [longs, line[0:9:2]] )     # longitude values for scene
            lats = np.concatenate( [lats, line[1:10:2]] )      # latitude values for scene
            
        lon_min = np.min(longs)
        lon_max = np.max(longs)
        lat_min = np.min(lats)
        lat_max = np.max(lats)        
        
    else:
        sys.exit("Error: both 'scene_file' and 'bbox' set to None (one needed to determine extents).")
    
    
    #=== Create DEM mosaic:
    if os.path.isdir(cmdargs.DEM_source):     # create DEM mosaic from DEM tiles (if needed)
        int_lon_min = int( np.floor( lon_min ) )      # corresponding lat/lon labels for DEM tiles
        int_lon_max = int( np.floor( lon_max ) )
        int_lat_min = int( np.floor( lat_min ) )
        int_lat_max = int( np.floor( lat_max ) )
        # dem_hgt_dir = cmdargs.DEM_source
        fstr = ''
        for lon in range(int_lon_min,int_lon_max+1):
            for lat in range(int_lat_min,int_lat_max+1):
                tmp = os.path.join(cmdargs.DEM_source, 'S' + str(np.abs(lat)) + 'E' + str(lon) + '.hgt')
                if os.path.exists(tmp):     # only add to mosaic if file exists
                    fstr += ' ' + tmp
        
        if fstr=='': sys.exit('There is no DEM data to mosaic.')
        
        # create DEM mosaic:
        res = os.system( 'gdalwarp -dstnodata 0 ' + fstr + ' ' + cmdargs.output_file )
        
    elif os.path.isfile(cmdargs.DEM_source):      # create DEM by subsetting
        # dem_in_file = cmdargs.DEM_source
        lon_min -= 0.02     # add some buffer...
        lon_max += 0.02
        lat_min -= 0.02
        lat_max += 0.02
        
        # create DEM subset:
        #cmd = 'gdal_translate -projwin %s %s %s %s' % (lon_min, lat_max, lon_max, lat_min)
        #cmd += ' -a_nodata 0 -projwin_srs EPSG:4326 %s %s' %(cmdargs.DEM_source, cmdargs.output_file)   # -of GTiff -co TILED=YES
        # gdalwarp will convert input nodata to 0
        cmd = 'gdalwarp -dstnodata 0 -te %s %s %s %s' % (lon_min, lat_min, lon_max, lat_max)
        cmd += ' -te_srs EPSG:4326 %s %s' %(cmdargs.DEM_source, cmdargs.output_file)
        res = os.system(cmd)
        
    else:
        sys.exit('Error generating DEM data')
    
    if res!=0: sys.exit("Could not create DEM mosaic.")
    
    
if __name__ == "__main__":
    main()
