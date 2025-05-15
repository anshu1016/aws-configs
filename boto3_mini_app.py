import boto3
import os
from botocore.exceptions import NoCredentialsError,ClientError


#S3 Configuration

BUCKET_NAME = 'ml-raw-data-demo1'
LOCAL_FOLDER = './upload_files'
s3 = boto3.client('s3')

def upload_files(filename):
    try:
        with open(os.path.join(LOCAL_FOLDER,filename),'rb') as f:
            s3.upload_fileobj(f,BUCKET_NAME,f"data/{filename}")
        print(f"Uploaded:{filename}")

    except FileNotFoundError:
        print(f" File Not Found {filename}")
    
    except NoCredentialsError:
        print(f"Credentials not available")
    except ClientError as e:
        print(f"Uploaded failed : {e}")

def list_files():
    try:
        print("These are the files present in bucket.")
        response = s3.list_objects_v2(Bucket=BUCKET_NAME,Prefix='data/')
        for obj in response.get('Contents',[]):
            print(f"-{obj['Key']}")
    except ClientError as e:
        print(f"List failed:{e}")


def download_file(s3_key,local_name):
    try:
        s3.download_file(BUCKET_NAME,s3_key,local_name)
        print(f"⬇️ Downloaded: {s3_key} to {local_name}")
    except ClientError as e:
        print(f"❌ Download failed: {e}")


def delete_file(s3_key):
    try:
        s3.delete_object(Bucket = BUCKET_NAME,Key=s3_key)
        print(f"🗑️ Deleted: {s3_key}")
    except ClientError as e:
        print(f"❌ Delete failed: {e}")



## Main Demo

if __name__ == "__main__":
    # 1. Upload a;; the files from the local folder
    print('Uploading files')
    if os.path.isdir(LOCAL_FOLDER):
        for file in os.listdir(LOCAL_FOLDER):
            upload_files(file)
    else:
        print(f"❌ Folder not found: {LOCAL_FOLDER}")

    #2. List files
    list_files()

    # 3. Download Example
    download_file("data/example.csv","downloaded_example.csv")

    # 4. Delete Example
    delete_file("data/example.csv")