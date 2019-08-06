#!/bin/bash
#PBS -N IntCoh

######################################################################
# . PBS shell script to be executed on NCI facilities.               #
# . This script is used for the interferometric coherence processing #
#   of SAR (Sentinel 1) data for ingestion into a SAR Data Cube.     #
# . Relies on Orbit Files and DEM data having been pre-downloaded    #
#   to a local drive prior to execution. A symbolic link is needed   #
#   between the user's .snap/auxdata/Orbit directory and the         #
#   directory where the Orbit data is stored (e.g. for the qd04      #
#   project:'/g/data1a/qd04/SNAP_Orbits_data').                      #
# . This script needs to be used in conjunction with the Python      #
#   code 'intcoh_proc_qsub.py', used for submitting a series of      #
#   jobs for processing of all scenes in a region and date range     #
#   of interest.                                                     #
# . This script expects: a file of scene pairs to process as         #
#   argument variable ARG_FILE_LIST, the path to the save directory  #
#   as argument variable BASE_SAVE_DIR, and the desired pixel        #
#   resolution as PIX_RES argument. Also, the '-l' PBS parameters    #
#   (e.g. ncpus, walltime, mem, etc.) are to be provided on the      #
#   command line upon job submission to PBS.                         #
# . The temporary data (e.g. DEM file, intermediate results)         #
#   created by this script are stored in each job's local file       #
#   system (i.e. inaccessible after job execution).                  #
######################################################################


module load gdal
module unload python3/3.4.3 python3/3.4.3-matplotlib	# to avoid error messages on VDI
module load python3/3.4.3 python3/3.4.3-matplotlib


# "hard-coded" files and folders:
xml_graph1=intcoh_proc_graph1.xml	# SNAP / GPT processing graph to use
xml_graph2=intcoh_proc_graph2.xml
xml_graph3=intcoh_proc_graph3.xml
xml_graph4=intcoh_proc_graph4.xml

N_CPUS_VDI=4	# nr of threads/CPUs to use on VDI (to keep CPU & MEM resources within limits)
VDI_TEMP_DIR=intcoh_proc_tmp_data	# directory of temporary processing data (no trailing /)

# input argument variables passed to the script:
if [ -z "$BASE_SAVE_DIR" ]; then	# base directory of processed scenes outputs (ends with /)
	echo "Error: BASE_SAVE_DIR variable is not set!"
	return
fi

if [ -z "$PIX_RES" ]; then			# pixel resolution
	echo "Error: PIX_RES variable is not set!"
	return
fi

if [ ! -f ${ARG_FILE_LIST} ]; then	# list of Sentinel scene pairs to process
	echo "Error: List of scene pairs does not exist!"
	return
fi

if [ -z "$GPT_EXEC" ]; then		# path to GPT executable not provided
	module load snap/6.0-py36
	GPT_EXEC=gpt
else	# path provided
	module unload snap/6.0-py36		# to avoid potential SW confusion...
fi

if [ -z "$DEM_SOURCE" ]; then	# source of DEM data
	echo "Error: DEM_SOURCE variable is not set!"
	return
fi


#=== Processing each scene pair in turn ===============================
tot=0
while read -r line; do	# count how many scenes we have
	tot=$((tot+1))
done < "${ARG_FILE_LIST}"

if [ ! -z "$VDI_JOB" ]; then	# job executed on VDI
	tmp_dir=$VDI_TEMP_DIR
else	# NCI -- use PBS_JOBFS
	tmp_dir=$PBS_JOBFS
fi

swaths=(IW1 IW2 IW3)
dem_out_file=$tmp_dir/DEM.tif
tmp_out_file1=$tmp_dir/tmp1.dim
tmp_out_data1=$tmp_dir/tmp1.data
tmp_out_file2s=($tmp_dir/${swaths[0]}_tmp2.dim $tmp_dir/${swaths[1]}_tmp2.dim $tmp_dir/${swaths[2]}_tmp2.dim)
tmp_out_data2s=($tmp_dir/${swaths[0]}_tmp2.data $tmp_dir/${swaths[1]}_tmp2.data $tmp_dir/${swaths[2]}_tmp2.data)
tmp_out_file3=$tmp_dir/tmp3.dim
tmp_out_data3=$tmp_dir/tmp3.data

cnt=0
OKscenes_cnt=0
notOKscenes_cnt=0
while read -r line; do
	# read file pairs & output filename from the list file:
	cnt=$((cnt+1))
	read pair_zipfile1 pair_zipfile2 out_filename <<< "$line"
	
	echo; echo "=== Processing SAR scene pair ============================================="
	echo Zip file pair nr. $cnt of $tot:
	echo " " $pair_zipfile1
	echo " " $pair_zipfile2
	
	# scene's processed file output: mimic folder structure in /g/data/fj7/ ... FOR FIRST SCENE IN CURRENT PAIR!
	tmp=${pair_zipfile1#*"Copernicus/"}
	tmp=$(dirname "$tmp")/
	output_file=${BASE_SAVE_DIR}$tmp$out_filename	# .dim file
	output_dir=${output_file/.dim/.data}
	if [ -d $output_dir ]; then rm -r $output_dir; fi
	if [ -f $output_file ]; then rm $output_file; fi
	
	# create DEM mosaic for current scene pair
	echo; echo "~~~ Generating DEM data for current pair ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	python3.4 make_DEM.py --scene_file ${pair_zipfile1},${pair_zipfile2} --DEM_source $DEM_SOURCE --output_file $dem_out_file
	if [ ! $? -eq 0 ]; then
		echo Scene processed \($cnt of $tot\): \#\#\# NOT OK -- error generating DEM data \#\#\#
		echo
		notOKscenes_arr[$notOKscenes_cnt]=$scene_zip_file
		notOKscenes_cnt=$((notOKscenes_cnt+1))
		continue
	fi
	
	# SNAP / GPT int. coh. processing of data, applied to each sub-swath:
	rptimes1=0; rptimes2=0
	uptimes1=0; uptimes2=0
	sptimes1=0; sptimes2=0
	for sw in 0 1 2; do
		swath=${swaths[$sw]}
	
		# step 1 - Split, Apply Orbit File, Back-Geocoding
		echo; echo "~~~ GPT processing, swath '${swath}' (graphs 1 of 4) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
		proc_cmd="$GPT_EXEC $xml_graph1 -Sscene1=$pair_zipfile1 -Sscene2=$pair_zipfile2 -Pswath=$swath -PexternalDEMFile=$dem_out_file -t $tmp_out_file1"
		if [ ! -z "$VDI_JOB" ]; then proc_cmd="$proc_cmd -q $N_CPUS_VDI"; fi	# job executed on VDI -- use only 4 CPUs
		echo $proc_cmd; echo
	
		exec 3>&1 4>&2
		proctimes=$( { time $proc_cmd 1>&3 2>&4; } 2>&1 )
		gpt_status=$?
		exec 3>&- 4>&-
	
		ptimes=($proctimes)	# real, user, sys times
		echo; echo "Elapsed processing times:"
		echo ${ptimes[0]} ${ptimes[1]}; echo ${ptimes[2]} ${ptimes[3]}; echo ${ptimes[4]} \ ${ptimes[5]}
		
		rptimes1=$(($rptimes1 + ${ptimes[1]%m*}))		# cumulative times in minutes for graph 1
		uptimes1=$(($uptimes1 + ${ptimes[3]%m*}))
		sptimes1=$(($sptimes1 + ${ptimes[5]%m*}))
		
		if [ ! $gpt_status -eq 0 ]; then break; fi
		
		# step 2 - Interferometric Calculation and Deburst
		echo; echo "~~~ GPT processing, swath '${swath}' (graphs 2 of 4) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
		proc_cmd="$GPT_EXEC $xml_graph2 -Sscene=$tmp_out_file1 -t ${tmp_out_file2s[$sw]}"
		if [ ! -z "$VDI_JOB" ]; then proc_cmd="$proc_cmd -q $N_CPUS_VDI"; fi	# job executed on VDI -- use only 4 CPUs
		echo; echo $proc_cmd; echo
	
		exec 3>&1 4>&2
		proctimes=$( { time $proc_cmd 1>&3 2>&4; } 2>&1 )
		gpt_status=$?
		exec 3>&- 4>&-
	
		ptimes=($proctimes)	# real, user, sys times
		echo; echo "Elapsed processing times:"
		echo ${ptimes[0]} ${ptimes[1]}; echo ${ptimes[2]} ${ptimes[3]}; echo ${ptimes[4]} \ ${ptimes[5]}
		
		rptimes2=$(($rptimes2 + ${ptimes[1]%m*}))		# cumulative times in minutes for graph 2
		uptimes2=$(($uptimes2 + ${ptimes[3]%m*}))
		sptimes2=$(($sptimes2 + ${ptimes[5]%m*}))
		
		if [ ! $gpt_status -eq 0 ]; then break; fi
	done
	
    # step 3 - Merge
	if [ $gpt_status -eq 0 ]; then
		echo; echo "~~~ GPT processing for current pair (graph 3 of 4) ~~~~~~~~~~~~~~~~~~~~~~~~"
		proc_cmd="$GPT_EXEC $xml_graph3 -Pfile1=${tmp_out_file2s[0]} -Pfile2=${tmp_out_file2s[1]} -Pfile3=${tmp_out_file2s[2]} -t $tmp_out_file3"
		if [ ! -z "$VDI_JOB" ]; then proc_cmd="$proc_cmd -q $N_CPUS_VDI"; fi	# job executed on VDI -- use only 4 CPUs
		echo $proc_cmd; echo
		
		exec 3>&1 4>&2
		proctimes=$( { time $proc_cmd 1>&3 2>&4; } 2>&1 )
		gpt_status=$?
		exec 3>&- 4>&-
		
		ptimes3=($proctimes)	# real, user, sys times
		echo; echo "Elapsed processing times:"
		echo ${ptimes3[0]} ${ptimes3[1]}; echo ${ptimes3[2]} ${ptimes3[3]}; echo ${ptimes3[4]} \ ${ptimes3[5]}
	fi		
		
	# step 4 - Multilook and Terrain Correction
	if [ $gpt_status -eq 0 ]; then
		echo; echo "~~~ GPT processing for current pair (graph 4 of 4) ~~~~~~~~~~~~~~~~~~~~~~~~"
		proc_cmd="$GPT_EXEC $xml_graph4 -Sscene=$tmp_out_file3 -PexternalDEMFile=$dem_out_file -PpixelSpacingInMeter=$PIX_RES -t $output_file"
		if [ ! -z "$VDI_JOB" ]; then proc_cmd="$proc_cmd -q $N_CPUS_VDI"; fi	# job executed on VDI -- use only 4 CPUs
		echo $proc_cmd; echo
		
		exec 3>&1 4>&2
		proctimes=$( { time $proc_cmd 1>&3 2>&4; } 2>&1 )
		gpt_status=$?
		exec 3>&- 4>&-
		
		ptimes4=($proctimes)	# real, user, sys times
		echo; echo "Elapsed processing times:"
		echo ${ptimes4[0]} ${ptimes4[1]}; echo ${ptimes4[2]} ${ptimes4[3]}; echo ${ptimes4[4]} \ ${ptimes4[5]}
	fi		
		
	# basic output check:
	# note: wildcards in the following statement work as they only match one file each...
	fout_status=1
	if [ -d $output_dir -a -f $output_file -a -f ${output_dir}/coh_VV_*_*.img -a -f ${output_dir}/coh_VV_*_*.hdr \
		 -a -f ${output_dir}/coh_VH_*_*.img -a -f ${output_dir}/coh_VH_*_*.hdr \
		 -a -f ${output_dir}/Intensity_ifg_VV_*_*.hdr -a -f ${output_dir}/Intensity_ifg_VV_*_*.img \
		 -a -f ${output_dir}/Intensity_ifg_VH_*_*.hdr -a -f ${output_dir}/Intensity_ifg_VH_*_*.img ]; then
		fout_status=0
	fi
	
	echo
	if [ $gpt_status -eq 0 -a $fout_status -eq 0 ]; then
		echo Scene processed \($cnt of $tot\): OK -- no abnormal GPT exit status\; all output files detected.
		rtimes1[$OKscenes_cnt]=$rptimes1	# real, user, sys times for graph 1
		utimes1[$OKscenes_cnt]=$uptimes1
		stimes1[$OKscenes_cnt]=$sptimes1
		rtimes2[$OKscenes_cnt]=$rptimes2	# real, user, sys times for graph 2
		utimes2[$OKscenes_cnt]=$uptimes2
		stimes2[$OKscenes_cnt]=$sptimes2
		rtimes3[$OKscenes_cnt]=${ptimes3[1]}	# real, user, sys times for graph 3
		utimes3[$OKscenes_cnt]=${ptimes3[3]}
		stimes3[$OKscenes_cnt]=${ptimes3[5]}
		rtimes4[$OKscenes_cnt]=${ptimes4[1]}	# real, user, sys times for graph 4
		utimes4[$OKscenes_cnt]=${ptimes4[3]}
		stimes4[$OKscenes_cnt]=${ptimes4[5]}
		OKscenes_cnt=$((OKscenes_cnt+1))
	else 
		notOKscenes_arr[$notOKscenes_cnt]=$scene_zip_file
		notOKscenes_cnt=$((notOKscenes_cnt+1))
		if [ $gpt_status -eq 0 ]; then
			echo Scene processed \($cnt of $tot\): \#\#\# NOT OK -- missing output file\(s\)! \#\#\#
		elif [ $fout_status -eq 0 ]; then
			echo Scene processed \($cnt of $tot\): \#\#\# NOT OK -- abnormal GPT exit status! \#\#\#
		else
			echo Scene processed \($cnt of $tot\): \#\#\# NOT OK -- abnormal GPT exit status and missing output file\(s\)! \#\#\#
		fi
	fi
	echo
	
	# change permissions of generated files:
	if [ -d $output_dir ]; then chmod -R g+rwX $output_dir; fi
	if [ -f $output_file ]; then chmod g+rw $output_file; fi
	
	# remove temporary files (not really necessary when using PBS_JOBFS)
	if [ -f $dem_out_file ]; then rm $dem_out_file; fi
	if [ -f $tmp_out_file1 ]; then rm $tmp_out_file1; fi
	if [ -d $tmp_out_data1 ]; then rm -r $tmp_out_data1; fi
	if [ -f $tmp_out_file3 ]; then rm $tmp_out_file3; fi
	if [ -d $tmp_out_data3 ]; then rm -r $tmp_out_data3; fi
	for sw in 0 1 2; do
		if [ -f ${tmp_out_file2s[$sw]} ]; then rm ${tmp_out_file2s[$sw]}; fi
		if [ -d ${tmp_out_data2s[$sw]} ]; then rm -r ${tmp_out_data2s[$sw]}; fi
	done
	
done < "$ARG_FILE_LIST"

# print some log info:
echo; echo "=== Summary info for this job ================================================="
echo Total nr of scenes: $tot
echo Total processed scenes: $cnt
echo "Total \"sucessful\" scenes (normal GPT exit code and output files detected):" $OKscenes_cnt

if [ ! $OKscenes_cnt -eq 0 ]; then
	echo; echo "Graph 1 -- (cumulative) processing times (in [min]) for \"successful\" scenes:"
	echo "  'Real' times:" ${rtimes1[@]}
	echo "  'User' times:" ${utimes1[@]}
	echo "  'System' times:" ${stimes1[@]}
	echo; echo "Graph 2 -- (cumulative) processing times (in [min]) for \"successful\" scenes:"
	echo "  'Real' times:" ${rtimes2[@]}
	echo "  'User' times:" ${utimes2[@]}
	echo "  'System' times:" ${stimes2[@]}
	echo; echo "Graph 3 -- processing times (in [min]) for \"successful\" scenes:"
	echo "  'Real' times:" ${rtimes3[@]%m*}
	echo "  'User' times:" ${utimes3[@]%m*}
	echo "  'System' times:" ${stimes3[@]%m*}
	echo; echo "Graph 4 -- processing times (in [min]) for \"successful\" scenes:"
	echo "  'Real' times:" ${rtimes4[@]%m*}
	echo "  'User' times:" ${utimes4[@]%m*}
	echo "  'System' times:" ${stimes4[@]%m*}
fi

if [ ! $notOKscenes_cnt -eq 0 ]; then
	echo; echo "List of failed scenes to re-process:"
	printf '%s\n' "${notOKscenes_arr[@]}"
fi
echo
