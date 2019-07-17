#!/bin/bash
#PBS -N PostProc
#PBS -q normal
#PBS -l walltime=20:00,ncpus=1,mem=3GB
#PBS -l wd,other=gdata1

####################################################################
# Script used to post-process the NCI outputs following the        #
# processing of SAR scenes using GPT. The updated SNAP 6.0 install #
# (as of Jan. 2019) generantes a large number of 'comment' lines   #
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
	sleep 120	# some buffer to ensure .out file is ready...
fi

# rename file to process:
tmp_out_file=${PROC_OUT_FILE}.bak
mv $PROC_OUT_FILE $tmp_out_file

# remove dodgy lines:
awk '
/^x = / {++cnt; next}
/^getGeoPos/ {++cnt; next}
/^(\.)+getGeoPos/ {++cnt; gsub(/getGeoPos.*/, ""); printf $0; next}
/\%(\.)*getGeoPos:/ {++cnt; gsub(/getGeoPos.*/, ""); printf $0; next} 
/^(\.)+x = / {++cnt; gsub(/x = .*/, ""); printf $0; next}
/\%(\.)*x = / {++cnt; gsub(/x = .*/, ""); printf $0; next}
{print $0}
END {print ""; print "Removed", cnt, "'GetGeoPos' error lines from original .out file."} 
' $tmp_out_file > $PROC_OUT_FILE

rm $tmp_out_file
