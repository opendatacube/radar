#!/usr/bin/env python

import requests
import argparse
try:
    from urllib import quote as urlquote # Python 2.X
except ImportError:
    from urllib.parse import quote as urlquote # Python 3+

import os

SARAQURL="https://copernicus.nci.org.au/sara.server/1.0/api/collections/S1/search.json?"

def quicklook_to_filepath(qlurl, validate):
    fp = "/g/data/fj7/Copernicus/Sentinel-1"+qlurl.split("Sentinel-1")[1].replace(".png",".zip")
    if validate:
        if not os.path.exists(fp): 
            print("Filepath doesn't exist:",fp)
            return None
    return fp

def main():
    parser = argparse.ArgumentParser(description="""
    Use SARA to find Sentinel-1 data in NCI filesystem
    """)
    parser.add_argument("--product", choices=['SLC','GRD'], default='GRD',
                        help="Data product to search. Default is %(default)s.")
    parser.add_argument("--mode", choices=['IW','EW'], default='IW',
                        help="Required sensor mode. Default is %(default)s.")
    parser.add_argument("--startdate",
                        help="Earliest date to search, as yyyy-mm-dd (default=%(default)s).")
    parser.add_argument("--enddate",
                        help="Latest date to search, as yyyy-mm-dd (default=%(default)s).")
    parser.add_argument("--polarisation", choices=['HH', 'VV', 'HH+HV', 'VH+VV'],
                        help="Required polarisation. Default will include any polarisations.")
    parser.add_argument("--orbitnumber", default=None, type=int,
                        help="Search in relative orbit number. Default will include any orbit number.")
    parser.add_argument("--orbitdirection", choices=['Ascending', 'Descending'], default=None,
                        help="Search in orbit direction. Default will include any orbit direction.")
    parser.add_argument("--bbox", nargs=4, type=float, default=[110.0, 155.0, -45.0, -10.0],
                        metavar=('westLong', 'eastLong', 'southLat', 'northLat'),
                        help=("Lat/long bounding box to search within. Default (%(default)s) covers Australia. "))
    parser.add_argument("--validatefilepaths", action='store_true',
                        help="Validate products exist at the filepaths.")
    parser.add_argument("--returnurls", action='store_true',
                        help="Return list of URLs for download, instead of local filepaths. Default is %(default)s.")
    parser.add_argument("--output", default="s1.list",
                        help="Filename for the output list of products. An exisiting file will be overwritten. Default is %(default)s.")
    parser.add_argument("--verbose", action='store_true')

    #parse options
    cmdargs = parser.parse_args()

    #construct search url
    queryUrl=SARAQURL
    if cmdargs.product:
        queryUrl +="&productType={0}".format(urlquote(cmdargs.product))
    if cmdargs.mode:
        queryUrl +="&sensorMode={0}".format(urlquote(cmdargs.mode))
    if cmdargs.startdate:
        queryUrl +="&startDate={0}".format(urlquote(cmdargs.startdate))
    if cmdargs.enddate:
        queryUrl +="&completionDate={0}".format(urlquote(cmdargs.enddate))
    if cmdargs.polarisation:
        queryUrl +="&polarisation={0}".format(urlquote(','.join(cmdargs.polarisation.split('+'))))
    if cmdargs.orbitnumber:
        queryUrl +="&orbitNumber={0}".format(urlquote('{0}'.format(cmdargs.orbitnumber)))
    if cmdargs.orbitdirection:
        queryUrl +="&orbitDirection={0}".format(urlquote(cmdargs.orbitdirection))
    if cmdargs.bbox:
        (westLong, eastLong, southLat, northLat) = cmdargs.bbox
        bboxWkt = 'POLYGON(({left} {top}, {right} {top}, {right} {bottom}, {left} {bottom}, {left} {top}))'.format(
            left=westLong, right=eastLong, top=northLat, bottom=southLat)
        queryUrl +="&geometry={0}".format(urlquote(bboxWkt))
    
    #make a paged query
    filepaths=[]
    queryUrl +="&maxRecords=50"
    page = 1
    if cmdargs.verbose: print(queryUrl)

    r = requests.get(queryUrl)
    result = r.json()
    nresult = result["properties"]["itemsPerPage"]
    while nresult>0:
        if cmdargs.verbose:
            print("Returned {0} products in page {1}.".format(nresult, page))

        #extract list of products
        if cmdargs.returnurls:
            filepaths +=[i["properties"]["services"]["download"]["url"] for i in result["features"]]
        else:
            filepaths +=[quicklook_to_filepath(i["properties"]["quicklook"], cmdargs.validatefilepaths) for i in result["features"]]
            
        #go to next page until nresult=0
        page +=1
        pagedUrl = queryUrl+"&page={0}".format(page)
        r = requests.get(pagedUrl)
        result = r.json()
        nresult = result["properties"]["itemsPerPage"]

    #write out the list of products
    with open(cmdargs.output,'w') as op:
        op.writelines(map(lambda x: x + '\n', filter(None,filepaths)))

if __name__ == "__main__":
    main()
