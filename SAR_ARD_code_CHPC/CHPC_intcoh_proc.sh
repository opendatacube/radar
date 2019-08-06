#!/bin/bash
#SBATCH --job-name=IntCoh

######################################################################
# . PBS shell script to be executed on CSIRO HPC facilities.         #
# . This script is used for the interferometric coherence processing #
#   of SAR (Sentinel 1) data for ingestion into a SAR Data Cube.     #
# . This script needs to be used in conjunction with the Python      #
#   code 'CHPC_intcoh_proc_qsub.py', used for submitting a series of #
#   jobs for processing of all pairs of scenes in a region and date  #
#   range of interest.                                               #
# . This script expects: a file of scene pairs to process as         #
#   argument variable ARG_FILE_LIST, the path to the save directory  #
#   as argument variable BASE_SAVE_DIR, the path GPT_EXEC to the GPT #
#   executable file on the CSIRO HPC system, and the desired pixel   #
#   resolution as PIX_RES argument. Also, the SBATCH parameters      #
#   (e.g. #cpus, walltime, mem, etc.) are to be provided on the      #
#   command line upon job submission to SLURM.                       #
######################################################################


module load python3/3.6.1


# "hard-coded" files and folders:
xml_graph1=CHPC_intcoh_proc_graph1.xml	# SNAP / GPT processing graph to use
xml_graph2=CHPC_intcoh_proc_graph2.xml
xml_graph3=CHPC_intcoh_proc_graph3.xml
xml_graph4=CHPC_intcoh_proc_graph4.xml

# input argument variables passed to the script:
if [ -z "$BASE_SAVE_DIR" ]; then	# base directory of processed pairs outputs (ends with /)
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
	echo "Error: GPT_EXEC variable is not set!"
	return
fi

# add 2 time strings of type XXmYY.YYYs
addTS () {
        t1=$1; t2=$2; m1=${t1%m*}; m2=${t2%m*}
        s1=$((echo $t1 | cut -d'm' -f 2) | cut -d's' -f 1)
        s2=$((echo $t2 | cut -d'm' -f 2) | cut -d's' -f 1)
        mt=$(($m1 + $m2)); st=$(echo $s1 + $s2 | bc)
        if (( $(echo "$st > 60.0" | bc) )); then
                st=$(echo "$st - 60.0" | bc)
                mt=$(($mt + 1))
        fi
        addedTS=${mt}m${st}s
}


#=== Processing each scene pair in turn ===============================
tot=0
while read -r line; do	# count how many pairs we have
	tot=$((tot+1))
done < "${ARG_FILE_LIST}"

swaths=(IW1 IW2 IW3)
tmp_out_file1=$MEMDIR/tmp1.dim
tmp_out_data1=$MEMDIR/tmp1.data
tmp_out_file2s=($MEMDIR/${swaths[0]}_tmp2.dim $MEMDIR/${swaths[1]}_tmp2.dim $MEMDIR/${swaths[2]}_tmp2.dim)
tmp_out_data2s=($MEMDIR/${swaths[0]}_tmp2.data $MEMDIR/${swaths[1]}_tmp2.data $MEMDIR/${swaths[2]}_tmp2.data)
tmp_out_file3=$MEMDIR/tmp3.dim
tmp_out_data3=$MEMDIR/tmp3.data

cnt=0
OKpairs_cnt=0
notOKpairs_cnt=0
while read -r line; do
	# read file pairs & output filename from the list file:
	cnt=$((cnt+1))
	read pair_zipfile1 pair_zipfile2 output_file <<< "$line"
	
	echo; echo "=== Processing SAR scene pair ============================================="
	echo Zip file pair nr. $cnt of $tot:
	echo " " $pair_zipfile1
	echo " " $pair_zipfile2
	
	# pair's processed file output:
	output_dir=${output_file/.dim/.data}
	if [ -d $output_dir ]; then rm -r $output_dir; fi
	if [ -f $output_file ]; then rm $output_file; fi
	
	# SNAP / GPT int. coh. processing of data, applied to each sub-swath:
	rptimes1=0m0.0s; rptimes2=0m0.0s
	uptimes1=0m0.0s; uptimes2=0m0.0s
	sptimes1=0m0.0s; sptimes2=0m0.0s
	for sw in 0 1 2; do
		swath=${swaths[$sw]}
	
		# step 1 - Split, Apply Orbit File, Back-Geocoding
		echo; echo "~~~ GPT processing, swath '${swath}' (graphs 1 of 4) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
		proc_cmd="$GPT_EXEC $xml_graph1 -Sscene1=$pair_zipfile1 -Sscene2=$pair_zipfile2 -Pswath=$swath -t $tmp_out_file1"
		echo $proc_cmd; echo
	
		exec 3>&1 4>&2
		proctimes=$( { time $proc_cmd 1>&3 2>&4; } 2>&1 )
		gpt_status=$?
		exec 3>&- 4>&-
	
		ptimes=($proctimes)	# real, user, sys times
		echo; echo "Elapsed processing times:"
		echo ${ptimes[0]} ${ptimes[1]}; echo ${ptimes[2]} ${ptimes[3]}; echo ${ptimes[4]} \ ${ptimes[5]}
		
		addTS $rptimes1 ${ptimes[1]}; rptimes1=$addedTS		# cumulative times in minutes for graph 1
		addTS $uptimes1 ${ptimes[3]}; uptimes1=$addedTS
		addTS $sptimes1 ${ptimes[5]}; sptimes1=$addedTS
		
		if [ ! $gpt_status -eq 0 ]; then break; fi
		
		# step 2 - Interferometric Calculation and Deburst
		echo; echo "~~~ GPT processing, swath '${swath}' (graphs 2 of 4) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
		proc_cmd="$GPT_EXEC $xml_graph2 -Sscene=$tmp_out_file1 -t ${tmp_out_file2s[$sw]}"
		echo; echo $proc_cmd; echo
	
		exec 3>&1 4>&2
		proctimes=$( { time $proc_cmd 1>&3 2>&4; } 2>&1 )
		gpt_status=$?
		exec 3>&- 4>&-
	
		ptimes=($proctimes)	# real, user, sys times
		echo; echo "Elapsed processing times:"
		echo ${ptimes[0]} ${ptimes[1]}; echo ${ptimes[2]} ${ptimes[3]}; echo ${ptimes[4]} \ ${ptimes[5]}
		
		addTS $rptimes2 ${ptimes[1]}; rptimes2=$addedTS		# cumulative times in minutes for graph 2
		addTS $uptimes2 ${ptimes[3]}; uptimes2=$addedTS
		addTS $sptimes2 ${ptimes[5]}; sptimes2=$addedTS
		
		if [ ! $gpt_status -eq 0 ]; then break; fi
	done
	
    # step 3 - Merge
	if [ $gpt_status -eq 0 ]; then
		echo; echo "~~~ GPT processing for current pair (graph 3 of 4) ~~~~~~~~~~~~~~~~~~~~~~~~"
		proc_cmd="$GPT_EXEC $xml_graph3 -Pfile1=${tmp_out_file2s[0]} -Pfile2=${tmp_out_file2s[1]} -Pfile3=${tmp_out_file2s[2]} -t $tmp_out_file3"
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
		proc_cmd="$GPT_EXEC $xml_graph4 -Sscene=$tmp_out_file3 -PpixelSpacingInMeter=$PIX_RES -t $output_file"
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
		echo Scene pair processed \($cnt of $tot\): OK -- no abnormal GPT exit status\; all output files detected.
		rtimes1[$OKpairs_cnt]=${rptimes1%.*}s	# real, user, sys times for graph 1
		utimes1[$OKpairs_cnt]=${uptimes1%.*}s
		stimes1[$OKpairs_cnt]=${sptimes1%.*}s
		rtimes2[$OKpairs_cnt]=${rptimes2%.*}s	# real, user, sys times for graph 2
		utimes2[$OKpairs_cnt]=${uptimes2%.*}s
		stimes2[$OKpairs_cnt]=${sptimes2%.*}s
		rtimes3[$OKpairs_cnt]=${ptimes3[1]%.*}s	# real, user, sys times for graph 3
		utimes3[$OKpairs_cnt]=${ptimes3[3]%.*}s
		stimes3[$OKpairs_cnt]=${ptimes3[5]%.*}s
		rtimes4[$OKpairs_cnt]=${ptimes4[1]%.*}s	# real, user, sys times for graph 4
		utimes4[$OKpairs_cnt]=${ptimes4[3]%.*}s
		stimes4[$OKpairs_cnt]=${ptimes4[5]%.*}s
		OKpairs_cnt=$((OKpairs_cnt+1))
	else 
		notOKpairs_arr[$notOKpairs_cnt]="$pair_zipfile1 $pair_zipfile2"
		notOKpairs_cnt=$((notOKpairs_cnt+1))
		if [ $gpt_status -eq 0 ]; then
			echo Scene pair processed \($cnt of $tot\): \#\#\# NOT OK -- missing output file\(s\)! \#\#\#
		elif [ $fout_status -eq 0 ]; then
			echo Scene pair processed \($cnt of $tot\): \#\#\# NOT OK -- abnormal GPT exit status! \#\#\#
		else
			echo Scene pair processed \($cnt of $tot\): \#\#\# NOT OK -- abnormal GPT exit status and missing output file\(s\)! \#\#\#
		fi
	fi
	echo
	
	# remove temporary files (not really necessary when using $LOCALDIR, $JOBDIR or $MEMDIR)
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
echo Total nr of pairs: $tot
echo Total processed pairs: $cnt
echo "Total \"sucessful\" pairs (normal GPT exit code and output files detected):" $OKpairs_cnt

if [ ! $OKpairs_cnt -eq 0 ]; then
	echo; echo "Graph 1 -- (cumulative) processing times for \"successful\" pairs:"
	echo "  'Real' times:" ${rtimes1[@]}
	echo "  'User' times:" ${utimes1[@]}
	echo "  'System' times:" ${stimes1[@]}
	echo; echo "Graph 2 -- (cumulative) processing times for \"successful\" pairs:"
	echo "  'Real' times:" ${rtimes2[@]}
	echo "  'User' times:" ${utimes2[@]}
	echo "  'System' times:" ${stimes2[@]}
	echo; echo "Graph 3 -- processing times for \"successful\" pairs:"
	echo "  'Real' times:" ${rtimes3[@]}
	echo "  'User' times:" ${utimes3[@]}
	echo "  'System' times:" ${stimes3[@]}
	echo; echo "Graph 4 -- processing times for \"successful\" pairs:"
	echo "  'Real' times:" ${rtimes4[@]}
	echo "  'User' times:" ${utimes4[@]}
	echo "  'System' times:" ${stimes4[@]}
fi

if [ ! $notOKpairs_cnt -eq 0 ]; then
	echo; echo "List of failed pairs to re-process:"
	printf '%s\n' "${notOKpairs_arr[@]}"
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
