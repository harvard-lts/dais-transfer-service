import boto3, os, os.path, logging
import transfer_ready_validation

s3 = boto3.resource('s3') 
logfile=os.getenv('LOGFILE_PATH', 'hdc3a_transfer_service')
loglevel=os.getenv('LOGLEVEL', 'WARNING')
logging.basicConfig(filename=logfile, level=loglevel)

def transfer_data(s3_bucket_name, s3_path, dropbox_dir):
    
    #Transfer
    perform_transfer(s3_bucket_name, s3_path, dropbox_dir)
    
    #TODO Validate transfer hash
    
    #TODO Cleanup s3
    
def perform_transfer(s3_bucket_name, s3_path, dropbox_dir):
    """
    Download the contents of a folder directory
    Args:
        s3_bucket_name: the name of the s3 bucket
        s3_path: the folder path in the s3 bucket
        dropbox_dir: an absolute directory path in the local file system
    """
    bucket = s3.Bucket(s3_bucket_name)
    for obj in bucket.objects.filter(Prefix=s3_path):
        target = os.path.join(dropbox_dir, os.path.relpath(obj.key, s3_path))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == '/':
            continue
        bucket.download_file(obj.key, target)
        
