import boto3
import io

bucket = 's3bucketname'
prefix = 'folder_name/20240223-filename.csv.zip'
s3_path = f"s3://{s3_bucket}/{filepath}"
unzip_prefix = 'swaps_repo_output/'

if filepath.endswith('.zip'):
    print(s3_path)
   
from zipfile import ZipFile
s3 = boto3.client("s3")

objects = s3.list_objects(
    Bucket=bucket,
    Prefix=prefix
)["Contents"]

unzipped_objects = s3.list_objects(
    Bucket=bucket,
    #Prefix=unzip_prefix
)["Contents"]



object_keys = [ o["Key"] for o in objects if o["Key"].endswith(".zip") ]

unzipped_object_keys = [ o["Key"] for o in unzipped_objects ]

print(object_keys)

print(unzipped_object_keys)               
                
for key in object_keys:
    obj = s3.get_object(Bucket = bucket,Key=key)
    
    objbuffer = io.BytesIO(obj["Body"].read())
    print(objbuffer)
    
    with ZipFile(objbuffer) as zip:
        filenames = zip.namelist()
        
        for filename in filenames:
            with zip.open(filename) as file:
                filepath = unzip_prefix + filename
                if filepath not in unzipped_object_keys:
                    s3.upload_fileobj(file,bucket,filepath)