#!/usr/bin/env python

## Python script used to create and submit batch jobs on the NCI for processing pairs 
## of Sentinel scenes (interferometric coherence).
## Based on 's1fromNCI.py' from Fang Yuan @ GA on opendatacube/radar GitHub.
## Based on 'S1_Read_Interferometry_Pairs.py' from Cate Ticehurst @ CSIRO.
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
##  > python3.4 intcoh_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-02-01
##  > python3.4 intcoh_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-02-01 --jobs_basename testing
##  > python3.4 intcoh_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-02-01 --pairs_per_job 3
## 
## TODO: . proper handling of print()
##       . proper handling of errors, sys.exit()
##       . provide cmd line info on submitted scripts, etc.
##       . more robust way to create output file names, get dates (perhaps from .xml file?)


import os, sys
import requests
import argparse
from datetime import datetime
import numpy as np
from xml.dom import minidom
try:
    from urllib import quote as urlquote # Python 2.X
except ImportError:
    from urllib.parse import quote as urlquote # Python 3+


SARAQURL = "https://copernicus.nci.org.au/sara.server/1.0/api/collections/S1/search.json?"

JOB_SCRIPT = "intcoh_proc.sh"  # name of PBS shell script to carry out the interferometric coherence processing
MAX_N_JOBS = 300            # https://opus.nci.org.au/display/Help/Raijin+User+Guide#RaijinUserGuide-QueueLimits

DEF_PAIRS_PER_JOB = 5       # default nr of scene pairs per job submitted to PBS
WALLTIME_PER_PAIR = 260     # in [min]; estimate of required walltime to process one scene pair on 'N_CPUS'
N_CPUS = 8                  # number of cpus to use for each PBS job
MEM_REQ = 80                # in [GB]; MEM (RAM) requirements for PBS job
MEM_JOBFS_REQ = 120         # in [GB]; job's local filesystem MEM requirements (e.g. for DEM file, intermediate data, etc.)


def quicklook_to_filepath(qlurl, validate):
    fp = "/g/data/fj7/Copernicus/Sentinel-1"+qlurl.split("Sentinel-1")[1].replace(".png",".zip")
    if validate and not os.path.exists(fp): 
        # print("Filepath doesn't exist:",fp)
        return None
    else:
        return fp

    
def main():
    tmp = "Interferometric coherence: create and submit batch jobs on the NCI for processing pairs of Sentinel scenes to ARD data."
    parser = argparse.ArgumentParser(description=tmp)
    
    # non-optional parameters:
    parser.add_argument("--startdate", default=None,
                        help="Earliest date to search, as yyyy-mm-dd. Non-optional parameter.")
    parser.add_argument("--enddate", default=None,
                        help="Latest date to search, as yyyy-mm-dd. Non-optional parameter.")
    parser.add_argument("--bbox", nargs=4, type=float, default=None,
                        metavar=('westLong', 'eastLong', 'southLat', 'northLat'),
                        help=("Lat/long bounding box to search within. Non-optional parameter."))
    
    # optional parameters:
    parser.add_argument( "--product", choices=['SLC','GRD'], default='SLC',
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
    
    parser.add_argument( "--pairs_per_job", default=DEF_PAIRS_PER_JOB, type=int,
                         help="Maximum number of scene pairs to process per PBS job. Default is %(default)s." )
    parser.add_argument( "--jobs_basename", 
                         help="Base name for submitted PBS jobs. Default is 'intcoh_proc_YYYYMMDD_HHMMSS' (current date and time)." )
    
    
    # parse options:
    cmdargs = parser.parse_args()
    
    if cmdargs.startdate is None or cmdargs.enddate is None or cmdargs.bbox is None:
        sys.exit("Input argumenrs 'startdate', 'enddate', and 'bbox' must be defined.")
    
    if cmdargs.jobs_basename is None:
        cmdargs.jobs_basename = 'intcoh_proc_' + str(datetime.now()).split('.')[0].replace('-','').replace(' ','_').replace(':','')
    
    
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
    
    
    # find interferometric pairs among the list of scenes:
    filepairs = []
    for ind1 in range(n_scenes):
        SAR_infile = filepaths[ind1]
        xml_file = SAR_infile.replace(".zip", ".xml")
        xml_ImageDoc = minidom.parse(xml_file)  # open the .xml file to read
        
        latlon = xml_ImageDoc.getElementsByTagName('CENTROID')
        lat = latlon[0].attributes['latitude'].value
        lon = latlon[0].attributes['longitude'].value
        
        OrbitNos = xml_ImageDoc.getElementsByTagName('ORBIT_NUMBERS')
        Abs_Orbit = OrbitNos[0].attributes['absolute'].value
        Rel_Orbit = OrbitNos[0].attributes['relative'].value

        # check SAR_infile with remaining files in the list to see if any are interferometry pairs:
        for ind2 in range(ind1+1, n_scenes):
            SAR_infile2 = filepaths[ind2]
            xml_file2 = SAR_infile2.replace(".zip", ".xml")
            xml_ImageDoc2 = minidom.parse(xml_file2)    # open the .xml file to read
            
            latlon2 = xml_ImageDoc2.getElementsByTagName('CENTROID')
            lat2 = latlon2[0].attributes['latitude'].value
            lon2 = latlon2[0].attributes['longitude'].value
            
            OrbitNos2 = xml_ImageDoc2.getElementsByTagName('ORBIT_NUMBERS')
            Abs_Orbit2 = OrbitNos2[0].attributes['absolute'].value
            Rel_Orbit2 = OrbitNos2[0].attributes['relative'].value
            
            tmp = abs( int(Abs_Orbit)-int(Abs_Orbit2) )
            if lat[:5]==lat2[:5] and lon[:5]==lon2[:5] and tmp<180 and Rel_Orbit==Rel_Orbit2:
                # output filename based on input filenames
                date1 = SAR_infile.split("__")[1].split("_")[1]
                date2 = SAR_infile2.split("__")[1].split("_")[1]
                tmp = SAR_infile.split("__")[1].split("_")[0]
                tmp2 = os.path.basename( SAR_infile.split("__")[0] )
                out_filename = tmp2 + "__" + tmp + "_" + date1 + "_" + date2 + "_IntCoh.dim"
                
                filepairs.append( SAR_infile2 + ' ' + SAR_infile + ' ' + out_filename )
    
    n_pairs = len(filepairs)
    if n_pairs==0: sys.exit("No pairs to process.")

    
    # write separate lists of scene pairs (one per PBS job) and submit PBS jobs:
    n_jobs = np.ceil( float(n_pairs) / cmdargs.pairs_per_job )
    if n_jobs>MAX_N_JOBS: sys.exit('Too many jobs for this query.')
    
    ind = 0
    for arr in np.array_split( filepairs, n_jobs ):
        ind += 1
        slist_name = cmdargs.jobs_basename + '_%03i.list' % ind
        
        # write list
        with open(slist_name,'w') as ln:
            ln.writelines( map(lambda x: x + '\n', arr) )
            
        # submit PBS job
        walltime = WALLTIME_PER_PAIR * len(arr)
        dlstr = "-l walltime=%i:00" % walltime
        dlstr += ",ncpus=%i" % N_CPUS
        dlstr += ",mem=%iGB" % MEM_REQ
        dlstr += ",jobfs=%iGB" % MEM_JOBFS_REQ
        dlstr += ",wd,other=gdata"
        dostr = "-o %s" % slist_name.replace('.list','.out')
        dostr += " -e %s" % slist_name.replace('.list','.err')
        dvstr = "-v ARG_FILE_LIST=%s" % slist_name
        
        cmd = 'qsub ' + dlstr +' '+ dostr +' '+ dvstr +' '+ JOB_SCRIPT 
        print( cmd )
        os.system( cmd )
        
    
if __name__ == "__main__":
    main()

