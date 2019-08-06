#!/bin/bash
#SBATCH --job-name=Backsc

#####################################################################
# . SBATCH shell script to be executed on CSIRO HPC facilities.     #
# . This script is used for the backscatter processing of SAR       #
#   (Sentinel 1) data for ingestion into a SAR Data Cube.           #
# . This script needs to be used in conjunction with the Python     #
#   code 'CHPC_backsc_proc_qsub.py', used for submitting a series   #
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
xml_graph=CHPC_backsc_proc_graph.xml				# SNAP / GPT processing graph to use

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
	
	# SNAP / GPT backscatter processing of data:
	echo; echo "~~~ GPT processing for current scene ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	proc_cmd="$GPT_EXEC $xml_graph -Sscene=$scene_zip_file -PpixelSpacingInMeter=$PIX_RES -t $output_file"
	echo $proc_cmd; echo
	
	exec 3>&1 4>&2
	proctimes=$( { time $proc_cmd 1>&3 2>&4; } 2>&1 )
	gpt_status=$?
	exec 3>&- 4>&-
	
	ptimes=($proctimes)	# real, user, sys times
	echo; echo "Elapsed processing times:"
	echo ${ptimes[0]} ${ptimes[1]}; echo ${ptimes[2]} ${ptimes[3]}; echo ${ptimes[4]} \ ${ptimes[5]}
	
	# basic output check:
	fout_status=1
	if [ -d $output_dir -a -f $output_file -a -f ${output_dir}/Gamma0_VH.hdr -a -f ${output_dir}/Gamma0_VH.img \
		 -a -f ${output_dir}/Gamma0_VV.hdr -a -f ${output_dir}/Gamma0_VV.img ]; then
		fout_status=0
	fi
	
	echo
	if [ $gpt_status -eq 0 -a $fout_status -eq 0 ]; then
		echo Scene processed \($cnt of $tot\): OK -- no abnormal GPT exit status\; all output files detected.
		rtimes[$OKscenes_cnt]=${ptimes[1]%.*}s		# real, user, sys times for this graph
		utimes[$OKscenes_cnt]=${ptimes[3]%.*}s
		stimes[$OKscenes_cnt]=${ptimes[5]%.*}s
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
	
done < "${ARG_FILE_LIST}"


# print some log info:
echo; echo "=== Summary info for this job ================================================="
echo Total nr of scenes: $tot
echo Total processed scenes: $cnt
echo "Total \"sucessful\" scenes (normal GPT exit code and output files detected):" $OKscenes_cnt

if [ ! $OKscenes_cnt -eq 0 ]; then
	echo; echo "Processing times for \"successful\" scenes:"
	echo "  'Real' times:" ${rtimes[@]}
	echo "  'User' times:" ${utimes[@]}
	echo "  'System' times:" ${stimes[@]}
fi

if [ ! $notOKscenes_cnt -eq 0 ]; then
	echo; echo "List of failed scenes to re-process:"
	printf '%s\n' "${notOKscenes_arr[@]}"
fi
echo


#=== Post-process dodgy .out file (output from terrain flattening step) ==================================
echo; echo "=== Post-processing job's .out file ==========================================="
proc_out_file=${ARG_FILE_LIST/.list/.out}
pproc_base=${ARG_FILE_LIST/.list/_postproc}

# proc_cmd="--output ${pproc_base}.out --error ${pproc_base}.err --export=PROC_OUT_FILE=$proc_out_file postproc_out_file.sh"
proc_cmd="--export=PROC_OUT_FILE=$proc_out_file postproc_out_file.sh"
jobid=$(sbatch --parsable --dependency=afterany:$SLURM_JOB_ID $proc_cmd)


#=== Record CPU & MEM info upon completion ===============================================================
tmp_script=${ARG_FILE_LIST/.list/__tmp.sh}
echo \#\!/bin/bash > $tmp_script
echo "if [ -n \"\$SLURM_JOB_ID\" ]; then sleep 30; fi" >> $tmp_script
echo echo -e \"\\n\\n*** CPU and MEM info ***\\n\" \>\> $proc_out_file >> $tmp_script
echo sacct -j $SLURM_JOB_ID --format=\"JobI,JobN,Pa,Stat,Allo,NN,No,E,MaxR,MaxV,ReqM,Ex,De\" \>\> $proc_out_file >> $tmp_script
echo "if [ -n \"\$SLURM_JOB_ID\" ]; then rm slurm-\$SLURM_JOB_ID.out; fi" >> $tmp_script
echo rm $tmp_script >> $tmp_script
chmod u+x $tmp_script
tmp=$(sbatch --dependency=afterany:$jobid --time=10 --mem=100M --ntasks-per-node=1 --job-name=CPUtimes $tmp_script)
