
import ijson,zipfile
import zipfile
import pandas as pd


data = []

with zipfile.ZipFile("/home/aayush-gyawali/Desktop/work/task_3/MagnaCarePPO_In-Network.zip", 'r') as z:
    for name in z.namelist():
        with z.open(name, 'r') as f:
            for item in ijson.items(f,'in_network.item'):
                data.append(item)

# print(data)

df = pd.DataFrame(data)
df.to_json("/home/aayush-gyawali/Desktop/work/task_3/innetwork.json",orient='records',indent=4)



# data = []

# with zipfile.ZipFile("/home/aayush-gyawali/Desktop/work/task_3/MagnaCarePPO_In-Network.zip", 'r') as z:
#     for name in z.namelist():
#         with z.open(name, 'r') as f:
#             for item in ijson.items(f, 'in_network.item.item'):
#                 data.append(item)

# print(data)
# df = pd.DataFrame(data)
# df.to_json("/home/aayush-gyawali/Desktop/work/provider.json",orient='records',indent=4)







