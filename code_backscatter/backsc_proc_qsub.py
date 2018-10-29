#!/usr/bin/env python

## Python script used to create and submit batch jobs on the NCI for processing Sentinel 
## backscatter data.
## Based on 's1fromNCI.py' from Fang Yuan @ GA on opendatacube/radar GitHub.
##
## This code is implemented in Python3, due to warning messages on the NCI from 
## urllib3 under Python2.7 (SNIMissingWarning and InsecurePlatformWarning -- "You can 
## upgrade to a newer version of Python to solve this"). The module 'requests' also
## has to be manually installed, which can be done as follows in a NCI terminal:
##   > module load python3/3.4.3 python3/3.4.3-matplotlib
##   > pip3.4 install -v --user requests
##
## Example:
##  > module load python3/3.4.3 python3/3.4.3-matplotlib
##  > python3.4 backsc_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-02-01
##  > python3.4 backsc_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-02-01 --jobs_basename testing
##  > python3.4 backsc_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-01-15 --scenes_per_job 3 --jobs_basename testing
## 
## TODO: . proper handling of print()
##       . proper handling of errors, sys.exit()
##       . provide cmd line info on submitted scripts, etc.
## 


import os, sys
import requests
import argparse
from datetime import datetime
import numpy as np
try:
    from urllib import quote as urlquote # Python 2.X
except ImportError:
    from urllib.parse import quote as urlquote # Python 3+


SARAQURL = "https://copernicus.nci.org.au/sara.server/1.0/api/collections/S1/search.json?"

JOB_SCRIPT = "backsc_proc.sh"  # name of PBS shell script to carry out the backscatter processing
MAX_N_JOBS = 300            # https://opus.nci.org.au/display/Help/Raijin+User+Guide#RaijinUserGuide-QueueLimits

DEF_SCENES_PER_JOB = 10     # default nr of scenes per job submitted to PBS
WALLTIME_PER_SCENE = 90     # in [min]; estimate of required walltime to process one scene on 'N_CPUS'
N_CPUS = 8                  # number of cpus to use for each PBS job
MEM_REQ = 48                # in [GB]; MEM requirements for PBS job


def quicklook_to_filepath(qlurl, validate):
    fp = "/g/data/fj7/Copernicus/Sentinel-1"+qlurl.split("Sentinel-1")[1].replace(".png",".zip")
    if validate and not os.path.exists(fp): 
        # print("Filepath doesn't exist:",fp)
        return None
    else:
        return fp

    
def main():
    parser = argparse.ArgumentParser(description="Backscatter processing: create and submit batch jobs on the NCI for processing Sentinel scenes to ARD data.")
    
    # non-optional parameters:
    parser.add_argument("--startdate", default=None,
                        help="Earliest date to search, as yyyy-mm-dd. Non-optional parameter.")
    parser.add_argument("--enddate", default=None,
                        help="Latest date to search, as yyyy-mm-dd. Non-optional parameter.")
    parser.add_argument("--bbox", nargs=4, type=float, default=None,
                        metavar=('westLong', 'eastLong', 'southLat', 'northLat'),
                        help=("Lat/long bounding box to search within. Non-optional parameter."))
    
    # optional parameters:
    parser.add_argument( "--product", choices=['SLC','GRD'], default='GRD',
                         help="Data product to search. Default is %(default)s." )
    parser.add_argument( "--mode", choices=['IW','EW'], default='IW',
                         help="Required sensor mode. Default is %(default)s." )
    parser.add_argument( "--polarisation", choices=['HH', 'VV', 'HH+HV', 'VH+VV'],
                         help="Required polarisation. Default will include any polarisations." )
    parser.add_argument( "--orbitnumber", default=None, type=int,
                         help="Search in relative orbit number. Default will include any orbit number." )
    parser.add_argument( "--orbitdirection", choices=['Ascending', 'Descending'], default=None,
                         help="Search in orbit direction. Default will include any orbit direction." )
    parser.add_argument( "--validatefilepaths", action='store_true', default=True,
                         help="Validate products exist at the filepaths. Default is %(default)s." )
    parser.add_argument( "--verbose", action='store_true',
                         help="Prints messages to screen. Default is %(default)s.")
    
    parser.add_argument( "--scenes_per_job", default=DEF_SCENES_PER_JOB, type=int,
                         help="Maximum number of scenes to process per PBS job. Default is %(default)s." )
    parser.add_argument( "--jobs_basename", 
                         help="Base name for submitted PBS jobs. Default is 'backsc_proc_YYYYMMDD_HHMMSS' (current date and time)." )
    
    
    # parse options:
    cmdargs = parser.parse_args()
    
    if cmdargs.startdate is None or cmdargs.enddate is None or cmdargs.bbox is None:
        sys.exit("Input argumenrs 'startdate', 'enddate', and 'bbox' must be defined.")
    
    if cmdargs.jobs_basename is None:
        cmdargs.jobs_basename = 'backsc_proc_' + str(datetime.now()).split('.')[0].replace('-','').replace(' ','_').replace(':','')
    
    
    # construct search url:
    queryUrl=SARAQURL
    if cmdargs.product:
        queryUrl += "&productType={0}".format(urlquote(cmdargs.product))
    if cmdargs.mode:
        queryUrl += "&sensorMode={0}".format(urlquote(cmdargs.mode))
    if cmdargs.startdate:
        queryUrl += "&startDate={0}".format(urlquote(cmdargs.startdate))
    if cmdargs.enddate:
        queryUrl += "&completionDate={0}".format(urlquote(cmdargs.enddate))
    if cmdargs.polarisation:
        queryUrl += "&polarisation={0}".format(urlquote(','.join(cmdargs.polarisation.split('+'))))
    if cmdargs.orbitnumber:
        queryUrl += "&orbitNumber={0}".format(urlquote('{0}'.format(cmdargs.orbitnumber)))
    if cmdargs.orbitdirection:
        queryUrl += "&orbitDirection={0}".format(urlquote(cmdargs.orbitdirection))
    if cmdargs.bbox:
        (westLong, eastLong, southLat, northLat) = cmdargs.bbox
        bboxWkt = 'POLYGON(({left} {top}, {right} {top}, {right} {bottom}, {left} {bottom}, {left} {top}))'.format(
                    left=westLong, right=eastLong, top=northLat, bottom=southLat )
        queryUrl += "&geometry={0}".format(urlquote(bboxWkt))
    
    
    # make a paged SARA query:
    filepaths = []
    queryUrl += "&maxRecords=50"
    page = 1
    if cmdargs.verbose: print(queryUrl)

    r = requests.get(queryUrl)
    result = r.json()
    nresult = result["properties"]["itemsPerPage"]
    while nresult>0:
        if cmdargs.verbose:
            print("Returned {0} products in page {1}.".format(nresult, page))

        # extract list of products
        filepaths += [quicklook_to_filepath(i["properties"]["quicklook"], cmdargs.validatefilepaths) for i in result["features"]]
            
        # go to next page until nresult=0
        page += 1
        pagedUrl = queryUrl+"&page={0}".format(page)
        r = requests.get(pagedUrl)
        result = r.json()
        nresult = result["properties"]["itemsPerPage"]

    # final list of products:
    filepaths = [ii for ii in filepaths if ii is not None]
    n_scenes = len(filepaths)
    
    
    # write separate lists of scenes (one per PBS job) and submit PBS job:
    n_jobs = np.ceil( float(n_scenes) / cmdargs.scenes_per_job )
    if n_jobs>MAX_N_JOBS: sys.exit('Too many jobs for this query.')
    
    ind = 0
    for arr in np.array_split( filepaths, n_jobs ):
        ind += 1
        slist_name = cmdargs.jobs_basename + '_%03i.list' % ind
        
        # write list
        with open(slist_name,'w') as ln:
            ln.writelines( map(lambda x: x + '\n', arr) )
            
        # submit PBS job
        walltime = WALLTIME_PER_SCENE * len(arr)
        dlstr = "-l walltime=%i:00" % walltime
        dlstr += ",ncpus=%i" % N_CPUS
        dlstr += ",mem=%iGB" % MEM_REQ
        dlstr += ",wd,other=gdata"
        dostr = "-o %s" % slist_name.replace('.list','.out')
        dostr += " -e %s" % slist_name.replace('.list','.err')
        dvstr = "-v ARG_FILE_LIST=%s" % slist_name
        
        cmd = 'qsub ' + dlstr +' '+ dostr +' '+ dvstr +' '+ JOB_SCRIPT 
        print( cmd )
        os.system( cmd )
        
    
if __name__ == "__main__":
    main()

