#!/bin/bash
#PBS -P qd04
#PBS -q normal

# other possible flags: # -m abe, -M email@domain.au

####################################################################
#  PBS shell script to be executed on NCI facilities.              #
#  This script is used for the dual-pol decomposition of SAR       #
#  (Sentinel 1) data for ingestion into a SAR Data Cube.           #
#  Relies on Orbit Files and DEM data having been pre-downloaded   #
#  to a local drive prior to execution. A symbolic link is needed  #
#  between the user's .snap/Orbit directory and the directory      #
#  where the Orbit data is stored.                                 #
#  This script needs to be used in conjunction with the Python     #
#  code 'dualpol_proc_qsub.py', used for submitting a series of    #
#  jobs for processing of all scenes in a region and date range    #
#  of interest.                                                    #
#  This script expects a file of scenes to process as argument     #
#  ARG_FILE_LIST. Also, the '-l' PBS parameters (e.g. ncpus,       #
#  walltime, mem, etc.) are to be provided on the command line     #
#  upon job submission to PBS.                                     #
#  The temporary data (e.g. DEM file, intermediate results)        #
#  created by this script are stored in each job's local file      #
#  system (i.e. inaccessible after job execution).                 #
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
xml_graph1=dualpol_proc_graph1.xml
xml_graph2=dualpol_proc_graph2.xml
xml_graph3=dualpol_proc_graph3.xml

# base directory of processed scenes outputs (end with /):
proc_output_dir=/g/data/qd04/Copernicus_DualPolDecomp/

# directory of SNAP's .hgt files of DEM data:
dem_hgt_dir=/g/data/qd04/SNAP_DEM_data


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
	dem_out_file=$PBS_JOBFS/${scene_basename%.*}.tif

	python make_DEM.py $scene_zip_file $dem_hgt_dir $dem_out_file
	
	# SNAP / GPT dual pol processing of data:
	tmp_out_file1=$PBS_JOBFS/${scene_basename%.*}_tmp1.dim
	tmp_out_data1=$PBS_JOBFS/${scene_basename%.*}_tmp1.data
	echo gpt $xml_graph1 -Sscene=$scene_zip_file -t $tmp_out_file1 -q $PBS_NCPUS
	time gpt $xml_graph1 -Sscene=$scene_zip_file -t $tmp_out_file1 -q $PBS_NCPUS
	
	tmp_out_file2=$PBS_JOBFS/${scene_basename%.*}_tmp2.dim
	tmp_out_data2=$PBS_JOBFS/${scene_basename%.*}_tmp2.data
	echo gpt $xml_graph2 -Sscene=$tmp_out_file1 -t $tmp_out_file2 -q $PBS_NCPUS
	time gpt $xml_graph2 -Sscene=$tmp_out_file1 -t $tmp_out_file2 -q $PBS_NCPUS
	
	echo gpt $xml_graph3 -Sscene=$tmp_out_file2 -PexternalDEMFile=$dem_out_file -t $output_file -q $PBS_NCPUS
	time gpt $xml_graph3 -Sscene=$tmp_out_file2 -PexternalDEMFile=$dem_out_file -t $output_file -q $PBS_NCPUS
	
	# remove temporary files (not really necessary when using PBS_JOBFS)
	rm $dem_out_file $tmp_out_file1 $tmp_out_file2
	rm -r $tmp_out_data1
	rm -r $tmp_out_data2
	
done < "${ARG_FILE_LIST}"

