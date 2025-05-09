
import zipfile,os
import ijson
import json
from decimal import Decimal

def extract_it(zip_path):
    def con_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError

    output = os.path.join(os.getcwd(),'file')
    os.makedirs(output,exist_ok=True)

    innetwork_path =os.path.join(output,'in_network.json')
    provider_path = os.path.join(output,'provider.json')

    with open(provider_path, 'w') as prov_file, \
        open(innetwork_path, 'w') as in_file, \
        zipfile.ZipFile(zip_path, 'r') as file:

        for data in file.namelist():
            with file.open(data, 'r') as f:
                for item in ijson.items(f, 'in_network.item'):
                    in_file.write(json.dumps(item, default=con_decimal) + '\n')
                f.seek(0)
                for item in ijson.items(f, 'provider_references.item'):
                    prov_file.write(json.dumps(item, default=con_decimal) + '\n')
    

    return(provider_path,innetwork_path)


