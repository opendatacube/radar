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
##  > python make_pair_DEM.py pair_zipfile1 pair_zipfile2 dem_hgt_dir dem_out_file
##
## where: 
##   . 'pair_zipfileX' are the .zip files of Sentinel-1 scene data
##   . 'dem_hgt_dir' is the directory containing the SNAP .hgt DEM tiles
##   . 'dem_out_file' is the (temporary) file where the DEM mosaic will be stored
##

import numpy as np
import sys, os

def main():
    
    #=== Argument parsing:
    pair_zipfile1 = sys.argv[1]     # e.g. '/g/data/fj7/Copernicus/Sentinel-1/C-SAR/GRD/2015/2015-10/40S140E-45S145E/S1A_IW_GRDH_1SDV_20151015T192606_20151015T192629_008167_00B798_776D.zip'
    pair_zipfile2 = sys.argv[2]
    dem_hgt_dir = sys.argv[3]       # e.g. '/g/data/qd04/SNAP_DEM_data'
    dem_out_file = sys.argv[4]      # e.g. '/g/data/qd04/tmp/mosaic.tif'
    
    
    #=== Get the scenes' extents from .xml files:
    longs = np.array([])
    lats = np.array([])
    for zipfile in [pair_zipfile1, pair_zipfile2]:
        xml_file = zipfile.replace('.zip','.xml')

        with open (xml_file, 'rt') as infile:
            for line in infile:
                if line.find('POLYGON')!=-1: break  # find line with 'POLYGON' in it

        for cc in ['POLYGON','(',')','\n']:     # remove unwanted characters
            line = line.replace(cc,'')

        line = line.replace(',',' ').split()
        line = np.array( map(lambda x: float(x), line) )   # convert to array of numeric lat/lon coords
        
        longs = np.concatenate( [longs, line[0:9:2]] )     # longitude values for scene
        lats = np.concatenate( [lats, line[1:10:2]] )      # latitude values for scene
        
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
