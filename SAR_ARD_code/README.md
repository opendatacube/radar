# Processing SAR data to ARD

This code automates the processing of Sentinel-1 SAR data to ARD (analysis-ready data) products on the National Computing Infrastructure (NCI). The processing uses the graph processing tool (GPT) from the SNAP toolbox to produce backscatter (`backsc`), dual polarimetry decomposition (`dualpol`), and interferometric coherence (`intcoh`) ARD products. The code also has options to run the processing on the NCI's Virtual Desktop Infrastructure (VDI).


## Getting Started

The general approach used in the code is simlar for the three types of data products (`<task>` = `backsc`, `dualpol` or `intcoh`), and is as follows:
1. use the function `<task>_proc_qsub.py` with desired user input arguments (e.g. date range, spatial extent, etc.) to automatically generate and submit a number of PBS job scripts on Raijin
1. the jobs are executed on the NCI on the basis of the `<task>_proc.sh` scripts and the corresponding `.xml` files (executed by GPT)
1. upon completion of the jobs, the data is generated in the desired directory, and various ancillary and diagnostics files can be investigated.
 
A typical command-line execution of `<task>_proc_qsub.py` (e.g. for `<task>` = `backsc`) on the NCI looks like this:

```bash
module load python3/3.4.3 python3/3.4.3-matplotlib
python3.4 backsc_proc_qsub.py --bbox 145.5 146.5 -35.0 -34.0 --startdate 2018-01-01 --enddate 2018-01-30 --jobs_basename /g/data/prj999/user123/test_log/ --base_save_dir /g/data/prj999/user123/test_proc_output/
```

Please refer to the code files directly (comments and GLOBAL variables at the top of the files) as well as the documentation file `SAR ARD code summary.pdf` for more detailed information, including the various user parameters that can be defined.


## Prerequisites

### Installing SNAP

SNAP is installed as a module on the NCI, and running the code without further instructions will use that module automatically. The user should be aware that the version installed on the NCI through this module might not be the most recent or up-to-date.

Alternatively, SNAP can be manually installed (and updated) if needed. This gives the user more control as to what version is used, when to update the software, etc. As the VDI doesn't have SNAP installed, a manual install is also required on that platform. To do this, simply download the UNIX version of the Sentinel Toolboxes from the [ESA SNAP download page](https://step.esa.int/main/download/snap-download/), and run it on the desired platform.

**Note**: as the space in the users' NCI home directory is limited, SNAP might need to be installed in a different part of the file system (e.g. in `/g/data/`). Also, the installation (or using the SNAP module on the NCI) will automatically create a `.snap` folder in the user's home directory, which will potentially get filled with automatically downloaded data during the GPT processing (depending on how the software is used). In order to avoid potential space issues in the user's home directory, the `.snap` foler can be re-located to a different location in the file system (e.g. somewhere in `/g/data`); this can be done by edting the file `gpt.vmoptions` in the install directory `/path/to/SNAP/install/bin/gpt`, and adding the following line in it:

```bash
-Dsnap.userdir=/g/data/path/to/new/folder/.snap
```

### MEM requirements in SNAP

SNAP has an internal "user-defined" memory limit, which determines how much RAM can / will be used during execution of a SNAP / GPT processing sequence. The value of this memory limit is automatically determined upon installation of the software, and depends on the specific computational platform it is installed on. On the NCI and VDI systems, this limit appears to be set to 65GB originally.

The user can, however, modify this memory amount. This can be done by editing the file `gpt.vmoptions` in the user's `.snap` install directory (e.g. `~/.snap/bin/gpt`), and altering the following line accordingly:

```bash
-Xmx=65GB
```

The user needs to be aware of this internal limit as it determines how much RAM is used / requested by SNAP during the processing. Therefore, upon submitting to PBS, the user must ensure that the NCI job's MEM request is **larger** than this specific SNAP memory limit (currently set to 88GB in the code). 


### Python module `requests`

Another requirement for the code to work seamlessly is to have the Python module `requests` installed on the system. With Python3.4 (currently used in the code), this module seems to be currently missing and thus needs to be installed manually by the user. This can be done by executing the following in a terminal on the NCI / VDI:

```bash
module load python3/3.4.3 python3/3.4.3-matplotlib
pip3.4 install -v --user requests
```

## Linking SNAP to orbit and DEM files

A major issue with an execution of the code on the NCI is that the compute nodes do not have a network interface. This creates an issue with a standard processing where GPT attempts to automatically download the tiles of DEM data and orbit files required for the processing.

To circumvent this issue, the DEM and orbit data can be downloaded prior to execution of the code. This can be done using `get_DEM.sh` (edit it and run it only once to download all DEM tiles over Australia and place them into a specific directory) and `get_orbits.sh` (edit it and run it at regular intervals to update the database, in order to process the latest Sentinel-1 data).

**Note**: the DEM tiles, as well as a portion of the orbit files, have already been downloaded as part of the `qd04` project and can be found in `/g/data1a/qd04/SNAP_DEM_data` and `/g/data1a/qd04/SNAP_Orbits_data`, respectively.

After downloading these datasets, the code automatically uses the pre-downloaded DEM tiles to create the DEM data layer required by GPT during the processing (if you downloaded the DEM tiles to a specific folder, you will need to update the variable `DEF_DEM_DIR` in the `<task>_proc_qsub.py` code).

**Note**: as opposed to the orbit files, no way was found to directly point SNAP / GPT to a directory of pre-downloaded DEM tiles (SNAP still tries to connect to the ESA server, even if all tiles have been pre-downloaded); hence the adopted approach of compositing the tiles during execution and feeding the resulting DEM mosaic to GPT instead.

With the orbit files, GPT will automatically look for these in the user's `.snap` directory, created during the installation of the SNAP software (or when the NCI SNAP module is loaded / used). Therefore, for GPT to automatically use the pre-downloaded orbit files, the user needs to create a symbolic link from their `.snap/auxdata` directory to the repository where the files have been pre-downloaded:

```bash
ln -s /dir/path/to/orbit/files/ /user/abc123/.snap/auxdata/Orbits
```

This will link the directory of pre-downloaded orbit files (e.g. could be `/g/data/qd04/SNAP_Orbits_data`) to the user's `.snap/auxdata/Orbits` directory where GPT will be looking for the orbit data during processing.

# Miscellaneous

## Post-processing the backscatter `.out` files

Following an original installation of SNAP, a software update was performed to correct a bug which precluded the processing of Sentinel-1 data acquired post-March 2018. Subsequently, the terrain flattening step in the GPT processing sequence started producing large amounts of "GetGeoPos" messages during the execution of the backscatter code, all logged to the `.out` file while processing on the NCI (and thereby generating file sizes of several GB, thus precluding a further investigation of these files). 

To remedy this, the script `postproc_out_file.sh` is automatically executed (submitted as PBS job) upon completion of the backscatter routine `backsc_proc.sh`, and will automatically clean up the resulting `.out` files.

# Author
**Eric Lehmann**, CSIRO Data61, as part of the joint CSIRO -- GA SAR ARD DataCube project.
