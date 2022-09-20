import os, os.path, shutil, boto3
from botocore.exceptions import ClientError

def upload_sample_data(s3, s3_bucket, s3_path):
    '''Uploaods the sample data if it doesn't exist in the bucket alreaady'''
    try:
        s3.Object(s3_bucket, s3_path).load()
    except ClientError:
        #Upload the files if the directory doesn't exist
        sample_data_path = os.path.join("/home/appuser/tests/data", s3_path)
        for filename in os.listdir(sample_data_path):
    
            file_key_name = s3_path + '/' + filename
            local_name = sample_data_path + '/' + filename
            s3.meta.client.upload_file(local_name, s3_bucket, file_key_name)
  
            
def cleanup_dropbox(dir_path):
    '''Removes the data that was downloaded into the dropbox'''
    try:
        shutil.rmtree(dir_path)
    except OSError as e:
        print("Error: %s : %s" % (dir_path, e.strerror))