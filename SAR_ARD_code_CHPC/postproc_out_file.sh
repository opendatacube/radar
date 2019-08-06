#!/bin/bash
#SBATCH --job-name=PostProc
#SBATCH --mem=1GB
#SBATCH --time=20
#SBATCH --ntasks-per-node=1

####################################################################
# Script used to post-process the HPC outputs following the        #
# processing of SAR scenes using GPT. The updated SNAP 6.0 install #
# (as of Jan. 2019) generates a large number of 'comment' lines    #
# related to 'getGeoPos' errors or issues. This script can be used #
# to automatically remove these lines from the resulting .out      #
# file, drastically reducing its size on disk.                     #
####################################################################

# To run manually:
# PROC_OUT_FILE=./backsc_proc_20190107_124944_001.out; NO_DELAY=true; postproc_out_file.sh

if [ -z "$PROC_OUT_FILE" ]; then
    echo "PROC_OUT_FILE variable does not exist!"
	exit 1
fi

if [ -z "$NO_DELAY" ]; then
	NO_DELAY=false
fi

# ensure file to process is available after previous job's completion:
if [ "$NO_DELAY" = "true" ]; then
	if [ ! -f ${PROC_OUT_FILE} ]; then
		echo "File to process PROC_OUT_FILE does not exist!"
		exit 1
	fi
else
	while [ ! -f ${PROC_OUT_FILE} ]; do
		sleep 10
	done
	sleep 60	# some buffer to ensure .out file is ready...
fi

# rename file to process:
tmp_out_file=${PROC_OUT_FILE}.bak
mv $PROC_OUT_FILE $tmp_out_file

# remove dodgy lines:
cnt=0
while read -r line; do
	cnt=$((cnt+1))
	if [[ ! $line == getGeoPos* ]] && [[ ! $line == "x = "* ]] ; then 
		if [[ $line == ".x = "* ]] ; then
			echo -n "." >> $PROC_OUT_FILE
		elif [[ $line == "..x = "* ]] ; then
			echo -n ".." >> $PROC_OUT_FILE
		elif [[ $line == ".getGeoPos"* ]] ; then
			echo -n "." >> $PROC_OUT_FILE
		elif [[ $line == "..getGeoPos"* ]] ; then
			echo -n ".." >> $PROC_OUT_FILE
		elif [[ $line == *"%x = "* ]] ; then
			ind=`expr index "$line" "%x = "`
			echo -n ${line:0:$ind} >> $PROC_OUT_FILE
		elif [[ $line == *"%getGeoPos"* ]] ; then
			ind=`expr index "$line" "%getGeoPos"`
			echo -n ${line:0:$ind} >> $PROC_OUT_FILE
		elif [[ $line == *"%.x = "* ]] ; then
			ind=`expr index "$line" "%.x = "`
			echo -n ${line:0:$ind} >> $PROC_OUT_FILE
		elif [[ $line == *"%.getGeoPos"* ]] ; then
			ind=`expr index "$line" "%.getGeoPos"`
			echo -n ${line:0:$ind} >> $PROC_OUT_FILE
		else
			cnt=$((cnt-1))
			echo "$line" >> $PROC_OUT_FILE
		fi
	fi
done < "${tmp_out_file}"

# echo -e "\n" >> $PROC_OUT_FILE
echo "Removed" $cnt "'GetGeoPos' error lines from original .out file." >> $PROC_OUT_FILE

rm $tmp_out_file
