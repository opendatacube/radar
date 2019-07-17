#!/usr/bin/env python

#####################################################################################
## Python script used to create and submit batch jobs on the NCI for processing 
## Sentinel backscatter data. Based on 's1fromNCI.py' from Fang Yuan @ GA on 
## opendatacube/radar GitHub.
##
## This code is implemented in Python3, due to warning messages on the NCI from 
## urllib3 under Python2.7 (SNIMissingWarning and InsecurePlatformWarning -- "You 
## can upgrade to a newer version of Python to solve this"). The module 'requests' 
## also has to be manually installed, which can be done as follows in an NCI 
## terminal:
##   > module load python3/3.4.3 python3/3.4.3-matplotlib
##   > pip3.4 install -v --user requests
##
## The list of possible input parameters (with typical examples) is provided below.
## All parameters are optional, unless otherwise specified.
##
##  --startdate 2018-01-01
##     Earliest date to search (inclusive), as yyyy-mm-dd. Non-optional parameter.
##  --enddate 2018-02-01
##     Latest date to search (NOT inclusive), as yyyy-mm-dd. Non-optional parameter.
##  --bbox 130.0 131.0 -21.0 -20.0
##     Lat/long bounding box to search within. Non-optional parameter.
##  --pixel_res 10.0
##     Pixel resolution in output product, in [m]. Default is 25.0 (DEF_PIXEL_RES in 
##     code below).
##  --DEM_source_file /path/to/dem.tif
##     Sets a source file of DEM data to use for processing; this DEM file will be 
##     subsetted to the relevant extents for each processed scene. If unused, the 
##     DEM data will be created from DEM tiles downloaded from the ESA/SNAP server.
##   
##  --product SLC
##     Sentinel-1 SAR data product to search / process -- choices are 'SLC' or 'GRD'. 
##     Default is 'SLC'.
##  --mode EW
##     Required Sentinel-1 SAR data sensor mode -- choices are 'IW' and 'EW'. Default  
##     is 'IW'.
##  --polarisation VV
##     Required Sentinel-1 SAR data polarisation -- choices are 'HH', 'VV', 'HH+HV',  
##     'VH+VV'. Default will include any polarisations.
##  --orbitnumber 123
##     Search for Sentinel-1 SAR data in a relative orbit number. Default will include 
##     any orbit number.
##  --orbitdirection Descending
##     Search for Sentinel-1 SAR data in a specific orbit direction. Choices are 
##     'Ascending', 'Descending'. Default will include any orbit direction.
##  --validatefilepaths
##     Validation that products actually exist at the filepaths. Default is to 
##     validate paths.
##  --verbose
##     Prints extra messages to screen. Default is not to print extra messages.
##  
##  --scenes_per_job 7
##     Maximum number of scenes to process per PBS job, to achieve sensible job 
##     resource requirements (walltime). Default is 5 (DEF_SCENES_PER_JOB in code 
##     below).
##  --jobs_basename ./test/job_543 
##     Base name or folder for the submitted PBS jobs. If it ends with '/' (i.e. a 
##     directory is provided), the default job name will be added to that path; the 
##     default name is 'dualpol_proc_YYYYMMDD_HHMMSS' (current date and time). If 
##     unused, the default 'jobs_basename' is set to the above default name.
##  --base_save_dir ./data_test
##     Base directory to save the processed data. Default is 
##     '/g/data1a/qd04/Copernicus_DualPolDecomp' (DEF_SAVE_DIR in code below).
## 
##  --submit_no_jobs
##     For debugging: use this flag to only display the PBS job commands, without 
##     actually submitting them to the NCI. Default is to submit PBS jobs.
##  --reprocess_existing
##     Use this flag to re-process scenes that already have existing output files in  
##     the save directory. Default is not to re-process existing data.
##  --gpt_exec ./gpt
##     Path to a local GPT executable (possibly a symlink). Default is to load GPT  
##     from the SNAP module.
##  --VDI_jobs
##     For debugging: use this flag to create a shell script for execution on VDI  
##     rather than submitting PBS jobs to the NCI. VDI jobs are hard-coded to always
##     use only 4 CPUs. Default is to submit NCI jobs.
##  --express_queue
##     For NCI jobs, submit to the 'express' queue instead of the normal queue. 
##     Default is to use the normal queue.
##  --nci_project pr99
##     For NCI jobs, submit the job from a specific project. Default is to use 
##     project qd04 (DEF_NCI_PROJECT in code below).
##  
## Examples:
##  > module load python3/3.4.3 python3/3.4.3-matplotlib
##  > python3.4 backsc_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-02-01
##  > python3.4 backsc_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-02-01 --jobs_basename /somedir/testing
##  > python3.4 backsc_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-02-01 --jobs_basename ./subdir/
##  > python3.4 backsc_proc_qsub.py --bbox 130.0 131.0 -21.0 -20.0 --startdate 2018-01-01 --enddate 2018-01-15 --scenes_per_job 3 --jobs_basename testing
##  > python3.4 backsc_proc_qsub.py --bbox 147.0 148.3 -33.8 -33.0 --startdate 2016-08-01 --enddate 2016-12-01 --base_save_dir /g/data/qd04/Copernicus_Backscatter_Lachlan/ --submit_no_jobs
## Testing post-March'18 data:
##  > python3.4 backsc_proc_qsub.py --bbox 147.0 148.3 -33.8 -33.0 --startdate 2018-06-01 --enddate 2018-06-15
##  > python3.4 backsc_proc_qsub.py --bbox 147.0 148.3 -33.8 -33.0 --startdate 2018-02-01 --enddate 2018-02-15
## Benchmarking:
##  > python3.4 backsc_proc_qsub.py --bbox 147.0 148.3 -33.8 -33.0 --startdate 2016-08-01 --enddate 2016-08-16
## 
## Production -- South East Victoria
##  > python3.4 backsc_proc_qsub.py --bbox 146.5 147.5 -38.2 -37.8 --startdate 2018-01-01 --enddate 2018-07-01 --jobs_basename ./log/ --base_save_dir /g/data1a/dz56/ga/ga_s1a_c_ard/1-0-0/Copernicus_Backscatter_SEVic/
## Production -- North West Victoria
##  > python3.4 backsc_proc_qsub.py --bbox 142.3 144.0 -35.75 -34.5 --startdate 2018-01-01 --enddate 2018-07-01 --jobs_basename ./log/ --base_save_dir /g/data1a/dz56/ga/ga_s1a_c_ard/1-0-0/Copernicus_Backscatter_NWVic/
## 
## NCI Testing:
##  > module load python3/3.4.3 python3/3.4.3-matplotlib
##  > python3.4 backsc_proc_qsub.py --reprocess_existing --express_queue --bbox 146.5 147.5 -38.2 -37.8 --startdate 2018-06-01 --enddate 2018-06-10 --jobs_basename ./log_backsc_test/ --base_save_dir /g/data1a/dz56/ga/ga_s1a_c_ard/1-0-0/Copernicus_backsc_TESTING
##  > python3.4 backsc_proc_qsub.py --reprocess_existing --express_queue --bbox 146.5 147.5 -38.2 -37.8 --startdate 2018-01-01 --enddate 2018-06-01 --jobs_basename ./log_backsc_test2/ --base_save_dir /g/data1a/dz56/ga/ga_s1a_c_ard/1-0-0/TESTING/backsc
## 
## VDI Testing:
##  > module load python3/3.4.3 python3/3.4.3-matplotlib
##  > python3.4 backsc_proc_qsub.py --VDI_jobs --reprocess_existing --bbox 146.5 147.5 -38.2 -37.8 --startdate 2018-06-01 --enddate 2018-06-10 --jobs_basename ./log_backsc_test/ --base_save_dir /g/data1a/dz56/ga/ga_s1a_c_ard/1-0-0/Copernicus_backsc_TESTING3 --gpt_exec /g/data/qd04/Eric/SNAP/snap/bin/gpt
## 
#####################################################################################


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
DEM_MAKE_FILE = "make_DEM.py"                   # for consistency checks only -- as used in JOB_SCRIPT
XML_GRAPH = "backsc_proc_graph.xml"            # for consistency checks only -- as defined in JOB_SCRIPT
ORBITS_DIR = "/g/data1a/qd04/SNAP_Orbits_data"  # for consistency checks only -- "usual" directory of orbit files...
DEF_PIXEL_RES = "25.0"      # string, default pixel resolution in output product (in [m])

DEF_SAVE_DIR = "/g/data/qd04/Copernicus_Backscatter"     # default base directory of processed scenes outputs
SOURCE_DIR = "/g/data/fj7/Copernicus/"      # "hard-coded" path to the Sentinel-1 data... (ends with '/')
SOURCE_SUBDIR = "Sentinel-1"                # "hard-coded" next subdir in the source path... (no trailing '/')
DEF_DEM_DIR = "/g/data1a/qd04/SNAP_DEM_data"    # default directory of DEM data (SNAP's .hgt files of DEM data)

DEF_SCENES_PER_JOB = 10     # default nr of scenes per job submitted to PBS
WALLTIME_PER_SCENE = 45     # in [min]; estimate of required walltime to process one scene on 'N_CPUS'
N_CPUS = 8                  # number of cpus to use for each PBS job
MEM_REQ = 88                # in [GB]; MEM (RAM) requirements for PBS job
MEM_JOBFS_REQ = 10          # in [GB]; job's local filesystem MEM requirements (for DEM file only)
MAX_N_JOBS = 300            # https://opus.nci.org.au/display/Help/Raijin+User+Guide#RaijinUserGuide-QueueLimits
DEF_QUEUE = "normal"        # default NCI queue to use
DEF_NCI_PROJECT = "qd04"    # default NCI project to use

# Note on MEM_REQ value: the SNAP software on the NCI is typically installed (both local install and when using 
# 'module load') with a definition of the maximum usable MEM allocation of 65GB (see -Xmx value in the folder:
# snap_install_dir/snap/bin/gpt.vmoptions). This means that the PBS jobs must be submitted with a minimum of 65GB 
# of MEM. It is further suggested to have the max MEM value (-Xmx) in SNAP set to ~75% of the total amount of RAM 
# in the system. According to this, the PBS jobs should theoretically be submitted with ~88GB of MEM.

def quicklook_to_filepath(qlurl, validate):
    fp = SOURCE_DIR + SOURCE_SUBDIR + qlurl.split(SOURCE_SUBDIR)[1].replace(".png",".zip")
    if validate and not os.path.isfile(fp): 
        # print("Filepath doesn't exist:",fp)
        return None
    else:
        return fp

    
def main():
    # basic input checks:
    if not os.path.isfile(DEM_MAKE_FILE): sys.exit("Error: DEM make file '%s' does not exist." % DEM_MAKE_FILE)
    if not os.path.isfile(JOB_SCRIPT): sys.exit("Error: job script '%s' does not exist." % JOB_SCRIPT)
    if not os.path.isfile(XML_GRAPH): sys.exit("Error: XML graph file '%s' does not exist." % XML_GRAPH)
    if not os.path.isdir(DEF_DEM_DIR): sys.exit("Error: DEM directory '%s' does not exist." % DEF_DEM_DIR)
    if not os.path.isdir(SOURCE_DIR): sys.exit("Error: Source directory '%s' does not exist." % SOURCE_DIR)
    if not os.path.isdir(SOURCE_DIR+SOURCE_SUBDIR): sys.exit("Error: Source directory '%s' does not exist." % (SOURCE_DIR+SOURCE_SUBDIR))
    
    
    # input parameters:
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
    parser.add_argument( "--pixel_res", default=DEF_PIXEL_RES, 
                         help="Pixel resolution in output product, in [m]. Default is %(default)s." )
    parser.add_argument( "--DEM_source_file", default=DEF_DEM_DIR, 
                         help="File of DEM data to use for the processing of SAR scenes. Default is to use of mosaic of DEM tiles from the ESA/SNAP server." )
    
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
                         help="Base name or dir for submitted PBS jobs. If ends with '/' (i.e. directory), default name will be added to the path. Default name is 'backsc_proc_YYYYMMDD_HHMMSS' (current date and time)." )
    parser.add_argument( "--base_save_dir", default=DEF_SAVE_DIR,
                         help="Base directory to save processed data. Default is %(default)s." )
    parser.add_argument( "--submit_no_jobs", action='store_true', 
                         help="Debug: use to only display job commands. Default is %(default)s." )
    parser.add_argument( "--reprocess_existing", action='store_true', 
                         help="Re-process already processed scenes with existing output files. Default is %(default)s." )
    parser.add_argument( "--gpt_exec", default=None,
                         help="Path to local GPT executable (possibly a symlink). Default is to load GPT from the SNAP module." )
    parser.add_argument( "--VDI_jobs", action='store_true', 
                         help="Debug: create shell scripts for execution on VDI rather than submitting NCI jobs. Default is %(default)s." )
    parser.add_argument( "--express_queue", action='store_true', 
                         help="Submit NCI jobs to the 'express' queue instead of the normal queue. Default is %(default)s." )
    parser.add_argument( "--nci_project", default=DEF_NCI_PROJECT, 
                         help="Submit NCI jobs from a specific project. Default is %(default)s." )
    
    
    # parse options:
    cmdargs = parser.parse_args()
    
    if cmdargs.startdate is None or cmdargs.enddate is None or cmdargs.bbox is None:
        sys.exit("Error: Input arguments 'startdate', 'enddate', and 'bbox' must be defined.")
    
    tmp = 'backsc_proc_' + str(datetime.now()).split('.')[0].replace('-','').replace(' ','_').replace(':','')
    if cmdargs.jobs_basename is None:
        cmdargs.jobs_basename = tmp
    elif cmdargs.jobs_basename.endswith("/"): 
        if not os.path.isdir(cmdargs.jobs_basename): os.mkdir(cmdargs.jobs_basename)
        cmdargs.jobs_basename += tmp
    
    if not cmdargs.base_save_dir.endswith("/"): cmdargs.base_save_dir += "/"
    
    if not cmdargs.gpt_exec is None:    # user provided path to local gpt exec
        if not os.path.isfile(cmdargs.gpt_exec):    # OK with symlinks
            sys.exit("Error: GPT executable '%s' does not exist." % cmdargs.gpt_exec)
        if not os.path.realpath(cmdargs.gpt_exec).endswith('gpt'):      # account for possible symlink
            sys.exit("Error: GPT executable '%s' does not point to executable named 'gpt'." % cmdargs.gpt_exec)
    
    if cmdargs.VDI_jobs and cmdargs.gpt_exec is None:
        sys.exit("VDI jobs execution selected but no path to GPT executable provided (VDI does not have a SNAP module installed).")
    
    
    # basic check for directory of orbit files:
    orb_dir = os.path.expanduser('~') + '/.snap/auxdata/Orbits'    # using SNAP module -- orbit files are in user's .snap dir
    if not cmdargs.gpt_exec is None:   # using local GPT / SNAP -- orbit files are potentially in custom dir, as per .vmoptions file
        tmp = cmdargs.gpt_exec + ".vmoptions"
        if not os.path.isfile(tmp): sys.exit("Local GPT .vmoptions file '%s' does not exist for executable '%s'." % (tmp,cmdargs.gpt_exec))
        usr_dir = None
        with open(tmp,'r') as fp:   # scan .vmoptions file for alternative user's .snap dir
            cline = fp.readline()
            while cline:
                if cline.startswith("-Dsnap.userdir"):
                    usr_dir = cline.split('=')[1][:-1]   # alternative path to user's .snap dir, e.g. '/home/usr/abc123/.snap'
                    break
                cline = fp.readline()
        
        if not usr_dir is None:     # we have a custom snap.userdir (o.w. revert to user's .snap dir)
            orb_dir = usr_dir + "/auxdata/Orbits"
    
    if not os.path.exists(orb_dir): sys.exit("Path to orbit files '%s' does not exist (GPT will likely malfunction)." % orb_dir)
    
    if os.path.islink( orb_dir ):   # user's path to orbit files is a symlink
        if not os.path.realpath(orb_dir)==os.path.normpath(ORBITS_DIR):
            print("Warning: user's path to orbit files '%s' does not point to usual directory (%s) -- ensure orbit files are up-to-date." % (orb_dir,ORBITS_DIR))
    else:
        if os.path.isdir( orb_dir ):    # user's path to orbit files is not symlink but is a dir
            print("Warning: user's path to orbit files '%s' is a directory (not symlink) -- ensure orbit files are up-to-date." % orb_dir)
        else:   # not symlink, not dir
            sys.exit("User's path to directory of orbit files '%s' does not exist (GPT will likely malfunction)." % orb_dir)
    
    
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
        bboxWkt = 'POLYGON(({left} {top}, {right} {top}, {right} {bottom}, {left} {bottom}, {left} {top}))'.format(left=westLong, right=eastLong, top=northLat, bottom=southLat )
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
    
    # if needed, exclude files already done:
    if not cmdargs.reprocess_existing:
        tmp = len(filepaths)
        filepaths = [f for f in filepaths if not os.path.isfile(f.replace(SOURCE_DIR,cmdargs.base_save_dir).replace('.zip','.dim'))]
        nproc = tmp - len(filepaths)
        if nproc!=0:
            print("A total of %i scenes (of %i) were found to be already processed (not re-processing)." % (nproc,tmp) )
    
    n_scenes = len(filepaths)
    if n_scenes==0: 
        print("Found no (new) scene to process.")
        return
    
    
    # write separate lists of scenes (one per PBS job):
    n_jobs = np.ceil( float(n_scenes) / cmdargs.scenes_per_job )
    if not cmdargs.VDI_jobs and n_jobs>MAX_N_JOBS: sys.exit('Error: Too many NCI jobs for this query.')
    jobs_arr = np.array_split( filepaths, n_jobs )
    
    # write lists:
    ind = 0
    for job in jobs_arr:        
        ind += 1
        slist_name = cmdargs.jobs_basename + '_%03i.list' % ind
        with open(slist_name,'w') as ln:
            ln.writelines( map(lambda x: x + '\n', job) )
    
    # create job scripts:
    if cmdargs.VDI_jobs:    # create shell script for execution on VDI
        out_fname = os.path.basename( cmdargs.jobs_basename ) + '.sh'   # create .sh file in same dir as other executables
        with open(out_fname,'w') as outf:
            outf.write( "#!/bin/bash\n\n" )
            outf.write( "## Batch jobs for BACKSCATTER processing of SAR scenes\n" )
            outf.write( "## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" )
            outf.write( "##\n")
            outf.write( "## Input parameters are:\n" )
            outf.write( "##   Start date: %s \n" % cmdargs.startdate )
            outf.write( "##   End date: %s \n" % cmdargs.enddate )
            outf.write( "##   Bounding box: %s \n" % str(cmdargs.bbox) )
            outf.write( "##   Base save dir: %s \n" % cmdargs.base_save_dir )
            outf.write( "##\n")
            outf.write( "## Current directory is: %s \n\n\n" % os.getcwd() )
            
            outf.write( "VDI_JOB=true\n" )
            outf.write( "BASE_SAVE_DIR=%s\n" % cmdargs.base_save_dir )
            outf.write( "PIX_RES=%s\n" % cmdargs.pixel_res )
            outf.write( "GPT_EXEC=%s\n\n" % cmdargs.gpt_exec )
            outf.write( "DEM_SOURCE=%s\n\n" % cmdargs.DEM_source_file )
    
            ind = 0
            for job in jobs_arr:    # write job
                ind += 1
                log_fname = cmdargs.jobs_basename + '_%03i.out' % ind
                slist_name = cmdargs.jobs_basename + '_%03i.list' % ind
                outf.write( "# Job nr. %i of %i ... \n" % (ind,int(n_jobs)) )
                outf.write( "ARG_FILE_LIST=%s\n" % slist_name )
                outf.write( ". %s 2>&1 | tee %s \n\n" % (JOB_SCRIPT,log_fname) )  # ... include stderr in log file
                # outf.write( ". %s | tee %s \n\n" % (JOB_SCRIPT,log_fname) )
    
    else:       # submit PBS jobs to NCI
        jlist_name = cmdargs.jobs_basename + '.jobs'
        with open(jlist_name,'w') as ln:
            ln.write( "\nBatch jobs for BACKSCATTER processing of SAR scenes\n" )
            ln.write( "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n" )
            ln.write( "Time is: %s \n\n" % str( datetime.now() ) )
            ln.write( "Input parameters are:\n" )
            ln.write( "  Start date: %s \n" % cmdargs.startdate )
            ln.write( "  End date: %s \n" % cmdargs.enddate )
            ln.write( "  Bounding box: %s \n" % str(cmdargs.bbox) )
            ln.write( "  Base save dir: %s \n\n" % cmdargs.base_save_dir )
            ln.write( "Current directory is:\n  %s \n\n" % os.getcwd() )
            ln.write( "Submitted jobs are:\n" )
        
        ind = 0
        for job in jobs_arr:    # submit PBS job
            ind += 1
            slist_name = cmdargs.jobs_basename + '_%03i' % ind
            
            walltime = WALLTIME_PER_SCENE * len(job)
            dlstr = "-l walltime=%i:00" % walltime
            dlstr += ",ncpus=%i" % N_CPUS
            dlstr += ",mem=%iGB" % MEM_REQ
            dlstr += ",jobfs=%iGB" % MEM_JOBFS_REQ
            dlstr += ",wd,other=gdata1"
            dostr = "-o %s" % (slist_name + '.out')
            dostr += " -e %s" % (slist_name + '.err')
            dvstr = "-v ARG_FILE_LIST=%s" % (slist_name + '.list')
            dvstr += ",BASE_SAVE_DIR=%s" % cmdargs.base_save_dir
            dvstr += ",PIX_RES=%s" % cmdargs.pixel_res
            dvstr += ",DEM_SOURCE=%s" % cmdargs.DEM_source_file
            dvstr += ",NCI_PROJ=%s" % cmdargs.nci_project
            if not cmdargs.gpt_exec is None:
                dvstr += ",GPT_EXEC=%s" % cmdargs.gpt_exec
            if cmdargs.express_queue:
                dlstr += " -q express"
            else:
                dlstr += " -q %s" % DEF_QUEUE
            dlstr += " -P %s" % cmdargs.nci_project
            
            cmd = 'qsub ' + dlstr + ' ' + dostr + ' ' + dvstr + ' ' + JOB_SCRIPT 
            cmdstr = 'Job nr. %03i of '%ind + str(int(n_jobs)) + ': ' + cmd
            print( cmdstr )
            with open(jlist_name,'a') as ln:
                ln.write( "  " + cmdstr + "\n" )
            if not cmdargs.submit_no_jobs: os.system( cmd )
    
    
if __name__ == "__main__":
    main()

