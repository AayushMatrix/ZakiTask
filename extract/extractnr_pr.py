import gzip
import json
import os
def extract(zip):
    output = os.path.join(os.getcwd(),'file')
    os.makedirs(output,exist_ok=True)
    new_path =os.path.join(output,'combined.json')   

    with open(new_path, 'w', encoding='utf-8') as out:
        for file in os.listdir(zip):
            if file.endswith(".json.gz"):
                with gzip.open(os.path.join(zip, file), 'rt', encoding='utf-8') as inp:
                    data = json.load(inp)
                    json.dump(data, out)
                    out.write('\n')
    return new_path