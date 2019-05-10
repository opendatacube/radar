#!/bin/bash

####################################################################
#  Shell script to download all Sentinel orbit files necessary     #
#  for the processing of SAR data using SNAP. Based on the code    #
#  provided by Fang Yuan @ GA. Typically executed on the VDI:      #
#    $> . get_orbits.sh                                            #       
#  Re-runs will check for existing files prior to downloading.     #
####################################################################


# Directory where the orbit files are stored locally:
target_dir=/g/data/qd04/SNAP_Orbits_data/Sentinel-1/


# List of orbit file directories used by SNAP:
declare -a listOfURLs=(
    "http://step.esa.int/auxdata/orbits/Sentinel-1/POEORB/S1A/"
    "http://step.esa.int/auxdata/orbits/Sentinel-1/POEORB/S1B/"
    "http://step.esa.int/auxdata/orbits/Sentinel-1/RESORB/S1A/"
    "http://step.esa.int/auxdata/orbits/Sentinel-1/RESORB/S1B/"
)


# Downloading orbit files:
for remoteURL in "${listOfURLs[@]}"
do
    # wget --quiet --mirror --no-parent --no-host-directories --cut-dirs=3 -R "index.html*" -P $target_dir $remoteURL
    wget --mirror --no-parent --no-host-directories --cut-dirs=3 -R "index.html*" -P $target_dir $remoteURL
done
