#!/bin/bash
#SBATCH --job-name=DualPol

#####################################################################
# . SBATCH shell script to be executed on CSIRO HPC facilities.     #
# . This script is used for the dual-pol decomposition of SAR       #
#   (Sentinel 1) data for ingestion into a SAR Data Cube.           #
# . This script needs to be used in conjunction with the Python     #
#   code 'CHPC_dualpol_proc_qsub.py', used for submitting a series  #
#   of jobs for processing of all scenes in a region and date range #
#   of interest.                                                    #
# . This script expects: a file of scenes to process as argument    #
#   variable ARG_FILE_LIST, the path to the save directory as       #
#   argument variable BASE_SAVE_DIR, the path GPT_EXEC to the GPT   #
#   executable file on the CSIRO HPC system, and the desired pixel  #
#   resolution as PIX_RES argument. Also, the SBATCH parameters     #
#   (e.g. #cpus, walltime, mem, etc.) are to be provided on the     #
#   command line upon job submission to SLURM.                      #
#####################################################################


module load python3/3.6.1


# "hard-coded" files and folders:
xml_graph1=CHPC_dualpol_proc_graph1.xml			# SNAP / GPT processing graph to use
xml_graph2=CHPC_dualpol_proc_graph2.xml
xml_graph3=CHPC_dualpol_proc_graph3.xml

# input argument variables passed to the script:
if [ -z "$BASE_SAVE_DIR" ]; then	# base directory of processed scenes outputs (ends with /)
	echo "Error: BASE_SAVE_DIR variable is not set!"
	return
fi

if [ -z "$PIX_RES" ]; then			# pixel resolution
	echo "Error: PIX_RES variable is not set!"
	return
fi

if [ ! -f ${ARG_FILE_LIST} ]; then	# list of Sentinel scenes/files to process
	echo "Error: List of scenes does not exist!"
	return
fi

if [ -z "$GPT_EXEC" ]; then		# path to GPT executable not provided
	echo "Error: GPT_EXEC variable is not set!"
	return
fi


#=== Processing each scene in turn ====================================
tot=0
while read -r line; do	# count how many scenes we have
	tot=$((tot+1))
done < "${ARG_FILE_LIST}"

tmp_out_file1=$MEMDIR/tmp1.dim
tmp_out_data1=$MEMDIR/tmp1.data
tmp_out_file2=$MEMDIR/tmp2.dim
tmp_out_data2=$MEMDIR/tmp2.data

cnt=0
OKscenes_cnt=0
notOKscenes_cnt=0
while read -r line; do
	# read scene's .zip file from list file:
	cnt=$((cnt+1))
	scene_zip_file="$line"
	
	echo; echo "=== Processing SAR scene =================================================="
	echo Zip file nr. $cnt of $tot:
	echo " " $scene_zip_file
	
	# scene's processed file output: mimic folder structure on thredds server
	tmp=${scene_zip_file#*"Sentinel-1/"}
	tmp=${BASE_SAVE_DIR}Sentinel-1/${tmp%%".zip"*}
	output_file=${tmp}.dim
	output_dir=${tmp}.data
	if [ -d $output_dir ]; then rm -r $output_dir; fi
	if [ -f $output_file ]; then rm $output_file; fi
	
	# SNAP / GPT dual pol processing of data:
	echo; echo "~~~ GPT processing for current scene (graph 1 of 3) ~~~~~~~~~~~~~~~~~~~~~~~"
	proc_cmd="$GPT_EXEC $xml_graph1 -Sscene=$scene_zip_file -t $tmp_out_file1"
	echo $proc_cmd; echo
	
	exec 3>&1 4>&2
	proctimes=$( { time $proc_cmd 1>&3 2>&4; } 2>&1 )
	gpt_status=$?
	exec 3>&- 4>&-
	
	ptimes1=($proctimes)	# real, user, sys times
	echo; echo "Elapsed processing times:"
	echo ${ptimes1[0]} ${ptimes1[1]}; echo ${ptimes1[2]} ${ptimes1[3]}; echo ${ptimes1[4]} \ ${ptimes1[5]}
	
	if [ $gpt_status -eq 0 ]; then
		echo; echo "~~~ GPT processing for current scene (graph 2 of 3) ~~~~~~~~~~~~~~~~~~~~~~~"
		proc_cmd="$GPT_EXEC $xml_graph2 -Sscene=$tmp_out_file1 -t $tmp_out_file2"
		echo $proc_cmd; echo
		
		exec 3>&1 4>&2
		proctimes=$( { time $proc_cmd 1>&3 2>&4; } 2>&1 )
		gpt_status=$?
		exec 3>&- 4>&-
		
		ptimes2=($proctimes)	# real, user, sys times
		echo; echo "Elapsed processing times:"
		echo ${ptimes2[0]} ${ptimes2[1]}; echo ${ptimes2[2]} ${ptimes2[3]}; echo ${ptimes2[4]} \ ${ptimes2[5]}
	fi
		
	if [ $gpt_status -eq 0 ]; then
		echo; echo "~~~ GPT processing for current scene (graph 3 of 3) ~~~~~~~~~~~~~~~~~~~~~~~"
		proc_cmd="$GPT_EXEC $xml_graph3 -Sscene=$tmp_out_file2 -PpixelSpacingInMeter=$PIX_RES -t $output_file"
		echo $proc_cmd; echo
		
		exec 3>&1 4>&2
		proctimes=$( { time $proc_cmd 1>&3 2>&4; } 2>&1 )
		gpt_status=$?
		exec 3>&- 4>&-
		
		ptimes3=($proctimes)	# real, user, sys times
		echo; echo "Elapsed processing times:"
		echo ${ptimes3[0]} ${ptimes3[1]}; echo ${ptimes3[2]} ${ptimes3[3]}; echo ${ptimes3[4]} \ ${ptimes3[5]}
	fi
	
	# basic output check:
	fout_status=1
	if [ -d $output_dir -a -f $output_file -a -f ${output_dir}/Alpha.hdr -a -f ${output_dir}/Alpha.img -a -f ${output_dir}/Anisotropy.hdr \
		 -a -f ${output_dir}/Anisotropy.img -a -f ${output_dir}/Entropy.hdr -a -f ${output_dir}/Entropy.img ]; then
		fout_status=0
	fi
	
	echo
	if [ $gpt_status -eq 0 -a $fout_status -eq 0 ]; then
		echo Scene processed \($cnt of $tot\): OK -- no abnormal GPT exit status\; all output files detected.
		rtimes1[$OKscenes_cnt]=${ptimes1[1]%.*}s	# real, user, sys times for graph 1
		utimes1[$OKscenes_cnt]=${ptimes1[3]%.*}s
		stimes1[$OKscenes_cnt]=${ptimes1[5]%.*}s
		rtimes2[$OKscenes_cnt]=${ptimes2[1]%.*}s	# real, user, sys times for graph 2
		utimes2[$OKscenes_cnt]=${ptimes2[3]%.*}s
		stimes2[$OKscenes_cnt]=${ptimes2[5]%.*}s
		rtimes3[$OKscenes_cnt]=${ptimes3[1]%.*}s	# real, user, sys times for graph 3
		utimes3[$OKscenes_cnt]=${ptimes3[3]%.*}s
		stimes3[$OKscenes_cnt]=${ptimes3[5]%.*}s
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
	
	# remove temporary files (not really necessary when using $LOCALDIR, $JOBDIR or $MEMDIR)
	if [ -f $tmp_out_file1 ]; then rm $tmp_out_file1; fi
	if [ -f $tmp_out_file2 ]; then rm $tmp_out_file2; fi
	if [ -d $tmp_out_data1 ]; then rm -r $tmp_out_data1; fi
	if [ -d $tmp_out_data2 ]; then rm -r $tmp_out_data2; fi
	
done < "${ARG_FILE_LIST}"


# print some log info:
echo; echo "=== Summary info for this job ================================================="
echo Total nr of scenes: $tot
echo Total processed scenes: $cnt
echo "Total \"sucessful\" scenes (normal GPT exit code and output files detected):" $OKscenes_cnt

if [ ! $OKscenes_cnt -eq 0 ]; then
	echo; echo "Graph 1 -- processing times for \"successful\" scenes:"
	echo "  'Real' times:" ${rtimes1[@]}
	echo "  'User' times:" ${utimes1[@]}
	echo "  'System' times:" ${stimes1[@]}
	echo; echo "Graph 2 -- processing times for \"successful\" scenes:"
	echo "  'Real' times:" ${rtimes2[@]}
	echo "  'User' times:" ${utimes2[@]}
	echo "  'System' times:" ${stimes2[@]}
	echo; echo "Graph 3 -- processing times for \"successful\" scenes:"
	echo "  'Real' times:" ${rtimes3[@]}
	echo "  'User' times:" ${utimes3[@]}
	echo "  'System' times:" ${stimes3[@]}
fi

if [ ! $notOKscenes_cnt -eq 0 ]; then
	echo; echo "List of failed scenes to re-process:"
	printf '%s\n' "${notOKscenes_arr[@]}"
fi
echo; echo


#=== Record CPU & MEM info upon completion ===============================================================
proc_out_file=${ARG_FILE_LIST/.list/.out}
tmp_script=${ARG_FILE_LIST/.list/__tmp.sh}
echo \#\!/bin/bash > $tmp_script
echo "if [ -n \"\$SLURM_JOB_ID\" ]; then sleep 30; fi" >> $tmp_script
echo echo -e \"\\n\\n*** CPU and MEM info ***\\n\" \>\> $proc_out_file >> $tmp_script
echo sacct -j $SLURM_JOB_ID --format=\"JobI,JobN,Pa,Stat,Allo,NN,No,E,MaxR,MaxV,ReqM,Ex,De\" \>\> $proc_out_file >> $tmp_script
echo "if [ -n \"\$SLURM_JOB_ID\" ]; then rm slurm-\$SLURM_JOB_ID.out; fi" >> $tmp_script
echo rm $tmp_script >> $tmp_script
chmod u+x $tmp_script
tmp=$(sbatch --dependency=afterany:$SLURM_JOB_ID --time=10 --mem=100M --ntasks-per-node=1 --job-name=CPUtimes $tmp_script)
