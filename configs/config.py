import json

def getConfig( param1):
	with open('config.json', 'r') as fcc_file:
    	fcc_data = json.load(fcc_file)
    return fcc_data[param1]