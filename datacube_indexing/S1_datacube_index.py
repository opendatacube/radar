# Add all SAR ARD scenes (containing .yaml files) within folder structure to the data cube

import os

directory = '/Sentinel-1/C-SAR/SLC/'
for root, dirs, files in os.walk(directory):
	for file in files:
		if file.endswith('.yaml'):
			infile=os.path.join(root,file)
			print("yaml file =",infile)
			command = 'datacube dataset add ' + infile
			os.system(command)
print('Finished!')