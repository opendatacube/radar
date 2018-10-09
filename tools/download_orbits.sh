#!/bin/bash

declare -a listOfURLs=(
    "http://step.esa.int/auxdata/orbits/Sentinel-1/POEORB/S1A/"
    "http://step.esa.int/auxdata/orbits/Sentinel-1/POEORB/S1B/"
    "http://step.esa.int/auxdata/orbits/Sentinel-1/RESORB/S1A/"
    "http://step.esa.int/auxdata/orbits/Sentinel-1/RESORB/S1B/"
)

for remoteURL in "${listOfURLs[@]}"
do
    wget --quiet --mirror --no-parent --no-host-directories --cut-dirs=3 -R "index.html*" $remoteURL
done
exit 0
