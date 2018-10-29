#!/bin/bash

####################################################################
#  Shell script to download all DEM files necessary for the        #
#  processing of SAR data using SNAP. Download all available zip   #
#  files of DEM data from ESA/SNAP for Australia, i.e. within      #
# [110.0, 155.0, -45.0, -10.0].                                    #
####################################################################


# Online directory of DEM data for SNAP:
snap_url=http://step.esa.int/auxdata/dem/SRTMGL1/

# Local directory where the DEM tiles are stored/unzipped:
target_dir=/g/data/qd04/SNAP_DEM_data/

# Local directory where the .zip files are stored:
zip_dir=${target_dir}zip/


# Downloading of DEM data:
for lon in `seq 110 155`
do

	for lat in `seq 10 45`;
	do
		# echo ${snap_dir}S${lat}E${lon}.SRTMGL1.hgt.zip
		# wget --quiet --mirror --no-host-directories --cut-dirs=3 -P $zip_dir ${snap_url}S${lat}E${lon}.SRTMGL1.hgt.zip
		# unzip -q -u -o ${zip_dir}S${lat}E${lon}.SRTMGL1.hgt.zip -d $target_dir
		wget --mirror --no-host-directories --cut-dirs=3 -P $zip_dir ${snap_url}S${lat}E${lon}.SRTMGL1.hgt.zip
		unzip -u -o ${zip_dir}S${lat}E${lon}.SRTMGL1.hgt.zip -d $target_dir
	done    
	
done
