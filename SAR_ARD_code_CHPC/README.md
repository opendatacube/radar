
# Processing SAR data to ARD on CSIRO HPC

This code automates the processing of Sentinel-1 SAR data to ARD (analysis-ready data) products on the CSIRO High-Performance Computing (CHPC) systems. The processing uses the graph processing tool (GPT) from the SNAP toolbox to produce backscatter (`backsc`), dual polarimetry decomposition (`dualpol`), and interferometric coherence (`intcoh`) ARD products.

The code base for this project is strongly correlated with the code available on the GitHub repository [ODC-Radar/SAR_ARD_Code_NCI](https://github.com/opendatacube/radar/tree/master/SAR_ARD_code_NCI), which was developed to perform similar processing of SAR data on the National Computational Infrastructure ([NCI](http://nci.org.au/)).


## Getting Started

The general approach used in the code is simlar for the three types of data products (`<task>` = `backsc`, `dualpol` or `intcoh`), and is as follows:
1. use the function `CHPC_<task>_proc_qsub.py` with desired user input arguments (e.g. date range, spatial extent, etc.) to automatically generate and submit a number of SBATCH job scripts on the HPC; this command will also automatically check the availability on disk of the selected scenes of raw Sentinel-1 data, and download them if necessary
1. the jobs are executed on the basis of the `CHPC_<task>_proc.sh` scripts and the corresponding `.xml` files (executed by GPT)
1. upon completion of the jobs, the data is generated in the desired directory, and various ancillary and diagnostics files can be investigated.

A typical command-line execution of `CHPC_<task>_proc_qsub.py` (e.g. for `<task>` = `backsc`) on 'Bracewell' looks like this:

```bash
module load python3/3.6.1
python3.6 CHPC_backsc_proc_qsub.py --bbox 145.5 146.5 -35.0 -34.0 --startdate 2018-01-01 --enddate 2018-01-30 --jobs_basename /flush1/abc123/test_log/ --base_data_dir /flush1/abc123/test_input_scenes/ --base_save_dir /flush1/abc123/test_proc_output/ --gpt_exec ~/snap/bin/gpt
```

Please refer to the code files directly (comments and GLOBAL variables at the top of the files) for more detailed information, including the various user parameters that can be defined.


## Installing SNAP

If SNAP isn't available on the selected HPC (e.g. not available on 'Bracewell'), SNAP can be manually installed (and updated) if needed. This gives the user full control as to what version is used, when to update the software, etc. To do this, simply download the UNIX version of the Sentinel Toolboxes from the [ESA SNAP download page](https://step.esa.int/main/download/snap-download/), and run it on the desired platform.

**Note**: after installation, a `.snap` folder will be created in the user's home directory, which will potentially get filled with automatically downloaded data during the GPT processing. In order to avoid potential space issues in the user's home directory, the `.snap` foler can be re-located to a different location in the file system (e.g. `/flush1`); this can be done by edting the file `gpt.vmoptions` in the install directory (e.g. `~/snap/bin/gpt`), and adding the following line in it:

```bash
-Dsnap.userdir=/flush1/user123/path/to/new/folder/.snap
```

## MEM requirements in SNAP

SNAP has an internal "user-defined" memory limit, which determines how much RAM can / will be used during execution of a SNAP / GPT processing sequence. The value of this memory limit is automatically determined upon installation of the software, and depends on the specific computational platform it is installed on. On 'Bracewell', for instance, this limit is originally set to 176GB.

The user can, however, modify this memory amount. This can be done by editing the file `gpt.vmoptions` in the user's `snap` install directory (e.g. `~/snap/bin/gpt`), and altering the following line accordingly:

```bash
-Xmx=176GB
```

The user needs to be aware of this internal limit as it determines how much RAM is used / requested by SNAP during the processing. Therefore, upon submitting to SBATCH, the user must ensure that the HPC job's MEM request is **larger** than this specific SNAP memory limit. 

In the implementation of the code above, **the internal SNAP MEM limit is assumed to be (re)set to `-Xmx=65GB`**, and the code submits SBATCH jobs with a MEM request of 100GB. Note that the processing sequence implemented in the current code base makes use of `$MEMDIR` on the HPC for the storage of temporary results, which contributes to the overall MEM usage in addition to the amount of RAM used by SNAP.


## Linking SNAP to orbit and DEM files

As the compute nodes on the CSIRO HPC systems have network access to the external world, the tiles of DEM data and orbit files will be automatically downloaded by GPT during execution. There is thus no need to specifically point GPT to pre-downloaded datasets, as done e.g. [on the NCI](https://github.com/opendatacube/radar/tree/master/SAR_ARD_code).


# Miscellaneous

## Post-processing the backscatter `.out` files

Following the original installation of SNAP, a software update needs to be performed to correct a bug which precludes the processing of Sentinel-1 data acquired post-March 2018. Subsequently, the terrain flattening step in the GPT processing sequence may start producing large amounts of "GetGeoPos" messages during the execution of the backscatter code, all logged to the `.out` file while processing on the HPC (and thereby generating file sizes of several GB, thus precluding a further investigation of these files). 

To remedy this, the script `postproc_out_file.sh` is automatically executed (submitted as PBS job) upon completion of the backscatter routine `backsc_proc.sh`, and will automatically clean up the resulting `.out` files.

## SNAP software update

If desired, an update of SNAP can be achieved by running the following command in a terminal:

```bash
cd /your/install/path/to/snap/bin
snap --nosplash --nogui --modules --update-all
```

When / if the update process reaches (and hangs at) the "updates = 0" stage, the user can then safely press Ctrl-C to terminate the update (apparently a known SNAP bug). 

**Note**: as well as fixing the above-mentioned post-March '18 issue, updating SNAP may potentially also break other parts of the SNAP processing routine! Updates of the SNAP software is at the user's own risk!

## Comparison with NCI

As part of the SAR-DataCube project (which led to the above code base), a comparison of the processing times was performed between an execution of the code on the NCI and the CSIRO HPC infrastructure ('Bracewell' selected for this exercise). A total of 10 Sentinel-1 scenes were processed for `backsc` and `dualpol` (7 scene pairs for `intcoh`) using identical parameters on these two platforms, with a varying number of CPUs. The processing times were recorded, and plotted in the form of box plots.

The various files in the `proc_times` folder provide these comparative results, which broadly demonstrate that the CSIRO HPC infrastructure is in general faster than the NCI in processing the SAR data. The extent of this, however, varies depending on the type of processing being executed: for instance, Bracewell is much faster for backscatter and interferometric coherence processing, and somewhat faster / on a par for dual poarimetric decomposition.

Note that for the `backsc` and `intcoh` results, the SNAP memory limit was defined as `-Xmx=65GB` (88GB job request) on both platforms, whereas for the `dualpol` results, Bracewell's memory limit was set to `-Xmx=120GB` (160GB job request). The file `BW_BW_comp_backsc.pdf` in the above folder however demonstrates that this RAM difference has negligible impact on the Bracewell computational results (at least for backscatter processing). A possible explanation is that once the allocated amount of memory is large enough for SNAP to fit all the processed data in RAM, further increasing the job's MEM request is unlikely to enable any further gains in terms of computational times.

Also, the SNAP version used on the CSIRO HPC was from the base install (as descrbed under "Installing SNAP"), whereas on the NCI, the SNAP software was (manually) updated once following installation.


# Author
**Eric Lehmann**, CSIRO Data61.
