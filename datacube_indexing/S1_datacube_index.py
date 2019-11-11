
# Index all SAR scenes (with .yaml files) within the given folder into data cube

import os

for root, dirs, files in os.walk('s1_gamma0_scene_yamls'):
	for file in files:
		if file.endswith('.yaml'):
			infile=os.path.join(root,file)
			print("yaml file =",infile)
			command = 'datacube -C radar.conf dataset add -p s1_gamma0_scene --confirm-ignore-lineage ' + infile
			os.system(command)

print('Finished gamma0!')
