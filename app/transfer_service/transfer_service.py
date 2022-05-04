import boto3, os, os.path, logging
from mqresources import mqutils
from botocore.exceptions import ClientError
import transfer_service.transfer_ready_validation as transfer_ready_validation
from transfer_service.transferexception import TransferException 

s3 = boto3.resource('s3') 
logfile=os.getenv('LOGFILE_PATH', 'hdc3a_transfer_service')
loglevel=os.getenv('LOGLEVEL', 'WARNING')
logging.basicConfig(filename=logfile, level=loglevel)

def transfer_data(message_data):
    s3_bucket_name = message_data['s3_bucket_name']
    s3_path = message_data['s3_path'] 
    dest_path = message_data['destination_path']
   
    if not path_exists(s3_bucket_name, s3_path):
        raise TransferException("The path {} does not exists in bucket {}.".format(s3_path, s3_bucket_name))  
           
    #Transfer
    perform_transfer(s3_bucket_name, s3_path, dest_path)
    
    #TODO Validate transfer hash
    transfer_succeeded=validate_transfer()
    if not transfer_succeeded:
       raise TransferException("Transfer failed")
    
    #TODO Notify transfer success
    transfer_status = mqutils.TransferStatus(message_data["package_id"], "success", dest_path)
    mqutils.notify_transfer_status_message(transfer_status)
            
    #Cleanup s3
    cleanup_s3()

def path_exists(s3_bucket, s3_path):
    try:
        s3_connect = boto3.client('s3', "us-east-1")
        res = s3_connect.list_objects_v2(
            Bucket=s3_bucket
        )
        for obj in res.get('Contents', []):
            if obj['Key'].startswith(s3_path + "/"):
                return True
        
        return False
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e
       
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
        
def validate_transfer():
    return True

def cleanup_s3(s3_bucket_name, s3_path):
    bucket = s3.Bucket(s3_bucket_name)
    if path_exists(s3_bucket_name, s3_path):
        resp = bucket.objects.filter(Prefix=s3_path).delete()
        print(resp)
    
        if (len(resp) == 0):
            raise TransferException("Nothing was deleted for {}/{}".format(s3_bucket_name, s3_path))
        if ('Errors' in resp[0]):
            raise TransferException("Errors occurred while attempting deletion for {}/{}:\n{}".format(s3_bucket_name, s3_path, resp['Errors']))
    else:
        logging.warn("Prefix {}/{} does not exist".format(s3_bucket_name, s3_path))
    
        
        
