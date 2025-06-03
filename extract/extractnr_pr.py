# import gzip
# import json
# import os
# def extract(zip):
#     output = os.path.join(os.getcwd(), 'file')
#     os.makedirs(output, exist_ok=True)
#     new_path = os.path.join(output, 'combined.json')
#     with open(new_path, 'w') as out:
#         for file in os.listdir(zip):
#             if file.endswith(".json.gz"):
#                 with gzip.open(os.path.join(zip, file), 'rt') as inp:
#                     data = json.load(inp)
#                     json.dump(data, out)
#                     out.write('\n')
#     return new_path
from decimal import Decimal
import gzip
import ijson
import os
import json
def extract(zip):
    def con_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError
    output = os.path.join(os.getcwd(), 'file')
    os.makedirs(output, exist_ok=True)
    new_path = os.path.join(output, 'combined.json')
    with open(new_path, 'w') as out:
        for file in os.listdir(zip):
            if file.endswith(".json.gz"):
                with gzip.open(os.path.join(zip, file), 'rt') as inp:
                    for record in ijson.items(inp, ''):
                        out.write(json.dumps(record, default=con_decimal) + '\n')
    return new_path



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("zip", help="Path to ZIP file containing .json.gz files")
    args = parser.parse_args()
    result_path = extract(args.zip)
