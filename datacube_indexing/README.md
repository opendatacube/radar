How to insert SAR ARD into a data cube:

On the National Computational Infrastructure (NCI), a test database has been set up in the Digital Earth Australia (DEA) development environment. 
An authorised user can access this test database with connection parameters defined in a configuration file (e.g. radar.conf).

In a terminal on the NCI, the DEA data cube can then be used with the following lines:

module use /g/data/v10/public/modules/modulefiles

module load dea

The data within the data cube can be accessed within a jupyter notebook, for example, with the following code:

import datacube

dc = datacube.Datacube(config=’radar.conf’)

Data cubes require YAML files to define product specifications and index into data cubes. 
The following describes the steps required in the creation of the three SAR ARD products in the data cube, including its indexing:

•	Define product:

$ datacube product add S1_XXX_productdef.yaml

where XXX is either ‘Backscatter’ (Gamma0 backscatter), ‘DualPolDecomp’ (a-h-alpha dual polarimetric decomposition) 
or ‘IntCoh’ (interferometric coherence). 

•	Create yaml file for each scene:

$ python S1_XXX_scene_yamls_multi.py

where XXX is ‘Backscatter’, DualPolDecomp’, or ‘IntCoherence’. 
This creates a .yaml file for each scene within the folder structure defined in the .py file. 
The S1_XXX_scene_yamls_multi.py program extracts all the relevant metadata information required for reading in the data cube, 
as well as retaining relevant metadata information required to meet CARD4L compliance.

•	Index each scene into datacube:

$ datacube dataset add SAR_ARD_file.yaml

This can be automated for multiple files using similar python code to S1_datacube_index.py.
