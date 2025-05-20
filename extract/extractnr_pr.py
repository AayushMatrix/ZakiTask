# import zipfile,os
# import gzip




# def gzipped(zip_path):
#     output = os.path.join(os.getcwd(),'file')
#     os.makedirs(output,exist_ok=True)
#     nrpr_path =os.path.join(output,'nrpr.json')


#     with open(nrpr_path, 'w') as outfile:
#         with zipfile.ZipFile(zip_path, 'r') as zip_ref:
#             gz_files = []
#             for f in zip_ref.namelist():
#                 if f.endswith('.json.gz'):
#                     gz_files.append(f)
#             first = True
#             for gz_file in gz_files:
#                 with zip_ref.open(gz_file) as f:
#                     json_content = gzip.decompress(f.read()).decode('utf-8')
#                     if not first:
#                         outfile.write(',\n')
#                     outfile.write(json_content)
#                     first = False

#     return nrpr_path


# import zipfile
# import gzip

# def gzipped(zip_file_path, output_file_path):
#     with open(output_file_path, 'w') as outfile:
#         with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
#             gz_files = []
#             for f in zip_ref.namelist():
#                 if f.endswith('.json.gz'):
#                     gz_files.append(f)
#             first = True
#             for gz_file in gz_files:
#                 with zip_ref.open(gz_file) as f:
#                     json_content = gzip.decompress(f.read()).decode('utf-8')
#                     if not first:
#                         outfile.write(',\n')
#                     outfile.write(json_content)
#                     first = False

# gzipped(
#     zip_file_path='/home/aayush-gyawali/Desktop/task/ZakiTask/ignore/highmark_mrf.zip',
#     output_file_path='/home/aayush-gyawali/Desktop/task/ZakiTask/ignore/streamed_output.json')


import zipfile
import gzip
import os

def gzipped(zip_file_path, output_file_path):
    # Verify the file exists and is a zip file
    if not os.path.exists(zip_file_path):
        raise FileNotFoundError(f"File not found: {zip_file_path}")
    
    if not zipfile.is_zipfile(zip_file_path):
        raise ValueError(f"Not a valid ZIP file: {zip_file_path}")

    with open(output_file_path, 'w') as outfile:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            gz_files = [f for f in zip_ref.namelist() if f.endswith('.json.gz')]
            first = True
            for gz_file in gz_files:
                with zip_ref.open(gz_file) as f:
                    json_content = gzip.decompress(f.read()).decode('utf-8')
                    if not first:
                        outfile.write(',\n')
                    outfile.write(json_content)
                    first = False

# Example usage
gzipped(
    zip_file_path='/home/aayush-gyawali/Desktop/task/ZakiTask/ignore/highmark_mrf.zip',
    output_file_path='/home/aayush-gyawali/Desktop/task/ZakiTask/ignore/streamed_output.json'
)