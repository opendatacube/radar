#!/bin/bash
#PBS -N Backsc

#####################################################################
# . PBS shell script to be executed on NCI facilities.              #
# . This script is used for the backscatter processing of SAR       #
#   (Sentinel 1) data for ingestion into a SAR Data Cube.           #
# . Relies on Orbit Files and DEM data having been pre-downloaded   #
#   to a local drive prior to execution. A symbolic link is needed  #
#   between the user's .snap/auxdata/Orbit directory and the        #
#   directory where the Orbit data is stored (e.g. for the qd04     #
#   project:'/g/data1a/qd04/SNAP_Orbits_data').                     #
# . This script needs to be used in conjunction with the Python     #
#   code 'backsc_proc_qsub.py', used for submitting a series of     #
#   jobs for processing of all scenes in a region and date range    #
#   of interest.                                                    #
# . This script expects: a file of scenes to process as argument    #
#   variable ARG_FILE_LIST, the path to the save directory as       #
#   argument variable BASE_SAVE_DIR, and the desired pixel          #
#   resolution as PIX_RES argument. Also, the '-l' PBS parameters   #
#   (e.g. ncpus, walltime, mem, etc.) are to be provided on the     #
#   command line upon job submission to PBS.                        #
# . The temporary data (e.g. DEM file, intermediate results)        #
#   created by this script are stored in each job's local file      #
#   system (i.e. inaccessible after job execution).                 #
#####################################################################


module use /g/data/v10/public/modules/modulefiles
module load dea
#module load gdal
#module unload python3/3.4.3 python3/3.4.3-matplotlib	# to avoid error messages on VDI
#module load python3/3.4.3 python3/3.4.3-matplotlib


# "hard-coded" files and folders:
xml_graph=backsc_proc_graph.xml				# SNAP / GPT processing graph to use

N_CPUS_VDI=4	# nr of threads/CPUs to use on VDI (to keep CPU & MEM resources within limits)
VDI_TEMP_DIR=backsc_proc_tmp_data	# directory of temporary processing data (no trailing /)

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
	module load snap/6.0-py36
	GPT_EXEC=gpt
else	# path provided
	module unload snap/6.0-py36		# to avoid potential SW confusion...
fi

proc_out_file=${ARG_FILE_LIST/.list/.out}
if [ ! -z "$VDI_JOB" ]; then	# job executed on VDI
	echo; echo If this file is corrupted by \'getGeoPos\' lines, execute the following to post-process it:
	echo "  PROC_OUT_FILE=$proc_out_file; NO_DELAY=true; postproc_out_file.sh"
	echo
fi

if [ -z "$DEM_SOURCE" ]; then	# source of DEM data
	echo "Error: DEM_SOURCE variable is not set!"
	return
fi


#=== Processing each scene in turn ====================================
tot=0
while read -r line; do	# count how many scenes we have
	tot=$((tot+1))
done < "${ARG_FILE_LIST}"

if [ ! -z "$VDI_JOB" ]; then	# job executed on VDI
	tmp_dir=$VDI_TEMP_DIR
	yaml_jobID_line="NCI job ID: N/A"
else	# NCI -- use PBS_JOBFS
	tmp_dir=$PBS_JOBFS
	yaml_jobID_line="NCI job ID: $PBS_JOBID"
fi
dem_out_file=$tmp_dir/DEM.tif


cnt=0
OKscenes_cnt=0
notOKscenes_cnt=0
while read -r line; do
	# read scene's .zip file from list file:
	cnt=$((cnt+1))
	read scene_zip_file SARAid <<< "$line"
	
	echo; echo "=== Processing SAR scene =================================================="
	echo Zip file nr. $cnt of $tot:
	echo " " $scene_zip_file
	
	# scene's processed file output: mimic folder structure in /g/data/fj7/
	output_file=${scene_zip_file#*"Copernicus/"}
	tmp=${BASE_SAVE_DIR}${output_file%%".zip"*}
	output_file=${tmp}.dim
	output_dir=${tmp}.data
	yaml_info_file="${tmp}_yaml.info"
	if [ -d $output_dir ]; then rm -r $output_dir; fi
	if [ -f $output_file ]; then rm $output_file; fi
	if [ -f $yaml_info_file ]; then rm $yaml_info_file; fi
	output_basedir=`dirname $output_file`
	if [ ! -d $output_basedir ]; then mkdir -p $output_basedir; fi
	
	# info to yaml file:
	echo "input scene: $scene_zip_file" > $yaml_info_file
	echo "input SARA ID: $SARAid" >> $yaml_info_file
	tmp=${scene_zip_file/.zip/.xml}
	if [ -f ${tmp} ]; then
		echo "input xml: $tmp" >> $yaml_info_file
	else 
		echo "input xml: N/A " >> $yaml_info_file
	fi
	echo "output file: $output_file" >> $yaml_info_file
	echo "output dir: $output_dir" >> $yaml_info_file
	echo "proc log file: $proc_out_file" >> $yaml_info_file
	
	# create DEM mosaic for current scene
	echo; echo "~~~ Generating DEM data for current scene ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	python make_DEM.py --scene_file $scene_zip_file --DEM_source $DEM_SOURCE --output_file $dem_out_file
	if [ ! $? -eq 0 ]; then
		echo Scene processed \($cnt of $tot\): \#\#\# NOT OK -- error generating DEM data \#\#\#
		echo "scene processed: NOT OK -- error generating DEM data" >> $yaml_info_file
		echo
		notOKscenes_arr[$notOKscenes_cnt]=$scene_zip_file
		notOKscenes_cnt=$((notOKscenes_cnt+1))
		
		# info to yaml file:
		echo "$yaml_jobID_line" >> $yaml_info_file
		echo "proc date: `date`" >> $yaml_info_file
		
		continue
	fi
	
	# SNAP / GPT backscatter processing of data:
	echo; echo "~~~ GPT processing for current scene ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
	proc_cmd="$GPT_EXEC $xml_graph -Sscene=$scene_zip_file -PexternalDEMFile=$dem_out_file -PpixelSpacingInMeter=$PIX_RES -t $output_file"
	if [ ! -z "$VDI_JOB" ]; then proc_cmd="$proc_cmd -q $N_CPUS_VDI"; fi	# job executed on VDI -- use only 4 CPUs
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
		echo "scene processed: OK -- no abnormal GPT exit status; all output files detected." >> $yaml_info_file
		rtimes[$OKscenes_cnt]=${ptimes[1]}		# real, user, sys times for this graph
		utimes[$OKscenes_cnt]=${ptimes[3]}
		stimes[$OKscenes_cnt]=${ptimes[5]}
		OKscenes_cnt=$((OKscenes_cnt+1))
	else 
		notOKscenes_arr[$notOKscenes_cnt]=$scene_zip_file
		notOKscenes_cnt=$((notOKscenes_cnt+1))
		if [ $gpt_status -eq 0 ]; then
			echo Scene processed \($cnt of $tot\): \#\#\# NOT OK -- missing output file\(s\)! \#\#\#
			echo "scene processed: NOT OK -- missing output file(s)!" >> $yaml_info_file
		elif [ $fout_status -eq 0 ]; then
			echo Scene processed \($cnt of $tot\): \#\#\# NOT OK -- abnormal GPT exit status! \#\#\#
			echo "scene processed: NOT OK -- abnormal GPT exit status!" >> $yaml_info_file
		else
			echo Scene processed \($cnt of $tot\): \#\#\# NOT OK -- abnormal GPT exit status and missing output file\(s\)! \#\#\#
			echo "scene processed: NOT OK -- abnormal GPT exit status and missing output file(s)!" >> $yaml_info_file
		fi
	fi
	echo
	
	# info to yaml file:
	echo "$yaml_jobID_line" >> $yaml_info_file
	echo "proc date: `date`" >> $yaml_info_file
	echo "SNAP modules and versions:" >> $yaml_info_file
	
	modarr=()
	for lind in `awk '/<MDATTR name="moduleName"/{ print NR }' $output_file`; do
		dline=$(awk 'NR==n {print; exit}' n=$lind $output_file)		# <MDATTR name="moduleName" type="ascii" mode="rw">SNAP Graph Processing Framework (GPF)</MDATTR>
		tmp=$(awk '{split($0,a,">"); print a[2]}' <<< $dline)		# SNAP Graph Processing Framework (GPF)</MDATTR
		mod=$(awk '{split($0,a,"<"); print a[1]}' <<< $tmp)			# SNAP Graph Processing Framework (GPF)
		
		lind=$((lind+1))
		dline=$(awk 'NR==n {print; exit}' n=$lind $output_file)		# <MDATTR name="moduleVersion" type="ascii" mode="rw">6.0.6</MDATTR>
		tmp=$(awk '{split($0,a,">"); print a[2]}' <<< $dline)		# 6.0.6</MDATTR
		ver=$(awk '{split($0,a,"<"); print a[1]}' <<< $tmp)			# 6.0.6
		modver="${mod}: ${ver}"
		
		found=0
		for mm in "${modarr[@]}"; do
			if [ "${mm}" = "${modver}" ]; then found=1; break; fi
		done		
		
		if [ $found -eq 0 ]; then
			modarr+=("$modver")
			echo "  $modver" >> $yaml_info_file
		fi
	done
		
	# change permissions of generated files:
	if [ -d $output_dir ]; then chmod -R g+rwX $output_dir; fi
	if [ -f $output_file ]; then chmod g+rw $output_file; fi
	if [ -f $yaml_info_file ]; then chmod g+rw $yaml_info_file; fi
	
	# remove temporary files (not really necessary when using PBS_JOBFS)
	if [ -f $dem_out_file ]; then rm $dem_out_file; fi
	
done < "${ARG_FILE_LIST}"


# print some log info:
echo; echo "=== Summary info for this job ================================================="
echo Total nr of scenes: $tot
echo Total processed scenes: $cnt
echo "Total \"sucessful\" scenes (normal GPT exit code and output files detected):" $OKscenes_cnt

if [ ! $OKscenes_cnt -eq 0 ]; then
	echo; echo "Processing times (in [min]) for \"successful\" scenes:"
	echo "  'Real' times:" ${rtimes[@]%m*}
	echo "  'User' times:" ${utimes[@]%m*}
	echo "  'System' times:" ${stimes[@]%m*}
fi

if [ ! $notOKscenes_cnt -eq 0 ]; then
	echo; echo "List of failed scenes to re-process:"
	printf '%s\n' "${notOKscenes_arr[@]}"
fi
echo


#=== Post-process dodgy .out file (output from terrain flattening step) ==================================
#if [ -z "$VDI_JOB" ]; then	# job executed on NCI
#	echo; echo "=== Post-processing job's .out file ==========================================="
#	proc_out_file=${ARG_FILE_LIST/.list/.out}
#	pproc_base=${ARG_FILE_LIST/.list/_postproc}
#	
#	qsub_cmd="qsub -o ${pproc_base}.out -e ${pproc_base}.err -v PROC_OUT_FILE=$proc_out_file -P $NCI_PROJ postproc_out_file.sh"
#	echo $qsub_cmd
#	$qsub_cmd
#	echo
#fi
