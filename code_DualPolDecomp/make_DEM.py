#!/usr/bin/env python

## Creates a mosaic of DEM data from SNAP's DEM tiles (if necessary). Executes 
## 'gdalwarp' as an operating system command (i.e. use 'module load gdal' prior to 
## executing this python script).
##
## TODO: . proper handling of input arguments (argparse)
##       . proper handling of errors and error messages (return NULL)
##       . general handling of potential errors...
##
## Function:
##  > python make_DEM.py scene_zip_file dem_hgt_dir dem_out_file
##  > python make_DEM.py S1A_IW_GRDH_1SDV_20151015T192606_20151015T192629_008167_00B798_776D.zip /g/data/qd04/SNAP_DEM_data ./testing.tif
##
## where: 
##   . 'scene_zip_file' is the .zip file of Sentinel-1 scene data
##   . 'dem_hgt_dir' is the directory containing the SNAP .hgt DEM tiles
##   . 'dem_out_file' is the (temporary) file where the DEM mosaic will be stored
##

import numpy as np
import sys, os

def main():
    
    #=== Argument parsing:
    scene_zip_file = sys.argv[1]    # e.g. '/g/data/fj7/Copernicus/Sentinel-1/C-SAR/GRD/2015/2015-10/40S140E-45S145E/S1A_IW_GRDH_1SDV_20151015T192606_20151015T192629_008167_00B798_776D.zip'
    dem_hgt_dir = sys.argv[2]       # e.g. '/g/data/qd04/SNAP_DEM_data'
    dem_out_file = sys.argv[3]      # e.g. '/g/data/qd04/tmp/mosaic.tif'
    
    
    #=== Get the scene's extents from .xml file:
    xml_file = scene_zip_file.replace('.zip','.xml')
    
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
    line = np.array( map(lambda x: float(x), line) )    # convert to array of numeric lat/lon coords

    longs = line[0:9:2]     # longitude values for scene
    lats = line[1:10:2]     # latitude values for scene
    dem_lon_min = int( np.floor( np.min(longs) ) )      # corresponding lat/lon labels for DEM tiles
    dem_lon_max = int( np.floor( np.max(longs) ) )
    dem_lat_min = int( np.floor( np.min(lats) ) )
    dem_lat_max = int( np.floor( np.max(lats) ) )

    
    #=== Create DEM mosaic (if needed):
    fstr = ''
    for lon in range(dem_lon_min,dem_lon_max+1):
        for lat in range(dem_lat_min,dem_lat_max+1):
            tmp = os.path.join(dem_hgt_dir, 'S' + str(np.abs(lat)) + 'E' + str(lon) + '.hgt')
            if os.path.exists(tmp):     # only add to mosaic if file exists
                fstr += ' ' + tmp
    
    if fstr=='': sys.exit('There is no DEM data to mosaic.')
    
    # create DEM mosaic:
    res = os.system( 'gdalwarp' + fstr + ' ' + dem_out_file )
    if res!=0: sys.exit("Could not create DEM mosaic.")
    

if __name__ == "__main__":
    main()
