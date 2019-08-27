#!/bin/bash

# make symlinks to orbital files so SNAP can access them

basedir="/g/data/fj7/Copernicus/Sentinel-1"
targetdir="$HOME/.snap/auxdata/Orbits/Sentinel-1"

for orb in "POEORB" "RESORB"
do
    for sat in "S1A" "S1B"
    do
	for year in $(seq 2014 $(date +"%Y"))
	do
	    for month in $(seq -f "%02g" 1 12)
	    do
		target=$targetdir/$orb/$sat/$year/$month;
		if [ ! -e $target ]; then
		    if [[ -n $(find $basedir/$orb/$sat -name $sat_*_$orb_*_V$year$month*.EOF*) ]]; then
			if [ ! -d $targetdir/$orb/$sat/$year ]; then
			    mkdir -p $targetdir/$orb/$sat/$year;
			fi
			ln -s $basedir/$orb/$sat $target;
		    fi
		fi
	    done
	done	
    done
done
exit 0
