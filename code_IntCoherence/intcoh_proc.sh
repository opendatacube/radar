#!/bin/bash
#PBS -P qd04
#PBS -q normal

# other possible flags: # -m abe, -M email@domain.au

#####################################################################
#  PBS shell script to be executed on NCI facilities.               #
#  This script is used for the interferometric coherence processing #
#  of SAR (Sentinel 1) data for ingestion into a SAR Data Cube.     #
#  Relies on Orbit Files and DEM data having been pre-downloaded    #
#  to a local drive prior to execution. A symbolic link is needed   #
#  between the user's .snap/Orbit directory and the directory       #
#  where the Orbit data is stored.                                  #
#  This script needs to be used in conjunction with the Python      #
#  code 'intcoh_proc_qsub.py', used for submitting a series of      #
#  jobs for processing of all scenes in a region and date range     #
#  of interest.                                                     #
#  This script expects a file of scene pairs to process as argument #
#  ARG_FILE_LIST. Also, the '-l' PBS parameters (e.g. ncpus,        #
#  walltime, mem, etc.) are to be provided on the command line      #
#  upon job submission to PBS.                                      #
#  The temporary data (e.g. DEM file, intermediate results)         #
#  created by this script are stored in each job's local file       #
#  system (i.e. inaccessible after job execution).                  #
#####################################################################

## TODO: . log file info, check for processing outputs / errors, time taken
##       . proper handling of errors & messages
##       . check user inputs, symlink to orbit files, etc.
##       . continued execution of scenes when one scene crashes
##       . check existing files; input argument to force overwrite


module load snap/6.0-py36
module load gdal
module load python


# SNAP / GPT processing graph to use:
xml_graph1=intcoh_proc_graph1.xml
xml_graph2=intcoh_proc_graph2.xml
xml_graph3=intcoh_proc_graph3.xml
xml_graph4=intcoh_proc_graph4.xml

# base directory of processed scenes outputs (end with /):
proc_output_dir=/g/data/qd04/Copernicus_IntCoherence/

# directory of SNAP's .hgt files of DEM data:
dem_hgt_dir=/g/data/qd04/SNAP_DEM_data


#=== File of Sentinel scenes =======================================
# List of file pairs to process, argument passed to the script:
if [ ! -f $ARG_FILE_LIST ]; then
    echo "List of scene pairs does not exist!"
	exit 1
fi


#=== Process each scene pair in turn ===============================
swaths="IW1 IW2 IW3" 
while read -r line
do
	# read file pairs & output filename from the list file:
	read pair_zipfile1 pair_zipfile2 out_filename <<< "$line"
	
	# scene's processed file output: mimic folder structure in /g/data/fj7/ ... FOR FIRST SCENE IN CURRENT PAIR!
	tmp=${pair_zipfile1#*"Copernicus/"}
	tmp=$(dirname "$tmp")/
	output_file=$proc_output_dir$tmp$out_filename
	
	# create DEM mosaic for current scene pair
	dem_out_file=$PBS_JOBFS/dem.tif
	python make_pair_DEM.py $pair_zipfile1 $pair_zipfile2 $dem_hgt_dir $dem_out_file
	
	
	# SNAP / GPT int. coh. processing of data, applied to each sub-swath:
	for swath in $swaths; do
		# step 1 - Split, Apply Orbit File, Back-Geocoding
		tmp_out_file1=$PBS_JOBFS/intcoh_tmp1.dim
		echo; echo $swath step 1.1: gpt $xml_graph1 -Sscene1=$pair_zipfile1 -Sscene2=$pair_zipfile2 -Pswath=$swath -PexternalDEMFile=$dem_out_file -t $tmp_out_file1 -q $PBS_NCPUS
		time gpt $xml_graph1 -Sscene1=$pair_zipfile1 -Sscene2=$pair_zipfile2 -Pswath=$swath -PexternalDEMFile=$dem_out_file -t $tmp_out_file1 -q $PBS_NCPUS
		
		# step 2 - Interferometric Calculation and Deburst
		tmp_out_file2=$PBS_JOBFS/intcoh_${swath}_tmp2.dim
		echo; echo $swath step 1.2: gpt $xml_graph2 -Sscene=$tmp_out_file1 -t $tmp_out_file2 -q $PBS_NCPUS
		time gpt $xml_graph2 -Sscene=$tmp_out_file1 -t $tmp_out_file2 -q $PBS_NCPUS
	done
	
    # step 3 - Merge
    in_file1=$PBS_JOBFS/intcoh_IW1_tmp2.dim
    in_file2=$PBS_JOBFS/intcoh_IW2_tmp2.dim
    in_file3=$PBS_JOBFS/intcoh_IW3_tmp2.dim
    tmp_out_file3=$PBS_JOBFS/intcoh_tmp3.dim
    tmp_out_data3=$PBS_JOBFS/intcoh_tmp3.data
	echo; echo Step 3: gpt $xml_graph3 -Pfile1=$in_file1 -Pfile2=$in_file2 -Pfile3=$in_file3 -t $tmp_out_file3 -q $PBS_NCPUS
	time gpt $xml_graph3 -Pfile1=$in_file1 -Pfile2=$in_file2 -Pfile3=$in_file3 -t $tmp_out_file3 -q $PBS_NCPUS
	
	# step 4 - Multilook and Terrain Correction
	echo; echo Step 4: gpt $xml_graph4 -Sscene=$tmp_out_file3 -PexternalDEMFile=$dem_out_file -t $output_file -q $PBS_NCPUS
	time gpt $xml_graph4 -Sscene=$tmp_out_file3 -PexternalDEMFile=$dem_out_file -t $output_file -q $PBS_NCPUS
	
	
	# remove temporary files (not really necessary when using PBS_JOBFS)
	for swath in $swaths; do
		rm $PBS_JOBFS/intcoh_${swath}_tmp2.dim
		rm -r $PBS_JOBFS/intcoh_${swath}_tmp2.data
	done
	
	rm $PBS_JOBFS/intcoh_tmp1.dim $dem_out_file $tmp_out_file3
	rm -r $PBS_JOBFS/intcoh_tmp1.data
	rm -r $tmp_out_data3
	
done < "$ARG_FILE_LIST"
