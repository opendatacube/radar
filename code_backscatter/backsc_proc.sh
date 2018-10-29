#!/bin/bash
#PBS -P qd04
#PBS -q normal

# other possible flags: # -m abe, -M email@domain.au

####################################################################
#  PBS shell script to be executed on NCI facilities.              #
#  This script is used for the backscatter processing of SAR       #
#  (Sentinel 1) data for ingestion into a SAR Data Cube.           #
#  Relies on Orbit Files and DEM data having been pre-downloaded   #
#  to a local drive prior to execution. A symbolic link is needed  #
#  between the user's .snap/Orbit directory and the directory      #
#  where the Orbit data is stored.                                 #
#  This script needs to be used in conjunction with the Python     #
#  code 'backsc_proc_qsub.py', used for submitting a series of     #
#  jobs for processing of all scenes in a region and date range    #
#  of interest.                                                    #
#  This script expects a file of scenes to process as argument     #
#  ARG_FILE_LIST. Also, the '-l' PBS parameters (e.g. ncpus,       #
#  walltime, mem, etc.) are to be provided on the command line     #
#  upon job submission to PBS.                                     #
####################################################################

## TODO: . log file info, check for processing outputs / errors, time taken
##       . proper handling of errors & messages
##       . check user inputs, symlink to orbit files, etc.
##       . continued execution of scenes when one scene crashes
##       . check existing files; input argument to force overwrite


module load snap/6.0-py36
module load gdal
module load python


# SNAP / GPT processing graph to use:
xml_graph=backsc_proc_graph.xml

# base directory of processed scenes outputs (end with /):
proc_output_dir=/g/data/qd04/Copernicus_backscatter/

# directory of SNAP's .hgt files of DEM data:
dem_hgt_dir=/g/data/qd04/SNAP_DEM_data

# directory where (temporary) DEM mosaic data is stored (end with /):
dem_out_dir=/g/data/qd04/tmp/


#=== File of Sentinel scenes =======================================
# List of files to process, argument passed to the script:
if [ ! -f ${ARG_FILE_LIST} ]; then
    echo "List of scenes does not exist!"
	exit 1
fi


#=== Process each scene in turn ====================================
while read -r line
do
	# read scene's .zip file from list file:
    scene_zip_file="$line"
    
	# scene's processed file output: mimic folder structure in /g/data/fj7/
	output_file=${scene_zip_file#*"Copernicus/"}
	output_file=${proc_output_dir}${output_file%%".zip"*}.dim
	
	# create DEM mosaic for current scene
	scene_basename=${scene_zip_file##*/}
	dem_out_file=$dem_out_dir${scene_basename%.*}.tif

	python make_DEM.py $scene_zip_file $dem_hgt_dir $dem_out_file
	
	# SNAP / GPT processing of backscatter data:
	echo gpt $xml_graph -Sscene=$scene_zip_file -PexternalDEMFile=$dem_out_file -t $output_file -q ${PBS_NCPUS}
	time gpt $xml_graph -Sscene=$scene_zip_file -PexternalDEMFile=$dem_out_file -t $output_file -q ${PBS_NCPUS}
	
	rm $dem_out_file
	
done < "${ARG_FILE_LIST}"

