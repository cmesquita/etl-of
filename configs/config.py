import json
import sys
import os

sys.path.append('.')

def getConfig( param1):
    with open('/Workspace/Repos/etl/etl-of/configs/config.json', 'r') as fcc_file:
        fcc_data = json.load(fcc_file)
    return fcc_data[param1]    