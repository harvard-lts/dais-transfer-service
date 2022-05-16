import boto3, os, os.path, logging, zipfile, glob
from mqresources import mqutils
from botocore.exceptions import ClientError
import transfer_service.transfer_ready_validation as transfer_ready_validation
from transfer_service.transferexception import TransferException 
from transfer_service.transferexception import ValidationException 
import transfer_service.transfer_validation as transfer_validation 

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
    
    zipextractionpath = unzip_transfer(dest_path)
    
    try:   
        #Type ValidationReturnValue
        validation_retval : transfer_validation.ValidationReturnValue  = transfer_validation.validate_transfer(zipextractionpath, dest_path) 
        #Validate transfer 
        if not validation_retval.isvalid:
            msg = "Transfer Validation Failed Gracefully:"
            msg = msg + "\n" + ','.join(validation_retval.get_error_messages())
            logging.error(msg)
            raise ValidationException(msg)
    except ValidationException as e:
        logging.exception("Transfer Validation Failed with Exception {}".format(str(e)))
        raise e
    
    #Notify transfer success
    transfer_status = mqutils.TransferStatus(message_data["package_id"], "success", dest_path)
    mqutils.notify_transfer_status_message(transfer_status)
            
    #Cleanup s3
    #TODO - once this is functioning end to end, uncomment
    #cleanup_s3(s3_bucket_name, s3_path)

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
    
    logging.debug("Transferring {}/{} to {}".format(s3_bucket_name, s3_path, dropbox_dir))
        
    bucket = s3.Bucket(s3_bucket_name)
    for obj in bucket.objects.filter(Prefix=s3_path):
        target = os.path.join(dropbox_dir, os.path.relpath(obj.key, s3_path))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == '/':
            continue
        logging.debug("Downloading {} to {}".format(obj.key, target))
        bucket.download_file(obj.key, target)
        
def unzip_transfer(fulldestpath):
    '''Unzips the transferred zipfile'''

    zip_file_re = r"{}/*.zip".format(fulldestpath)
    files = glob.glob(zip_file_re)
    print(files)
    if len(files) == 0:
        raise Exception("No zip files found in {}".format(fulldestpath))
    elif len(files) > 1:
        raise Exception("{} zip files found in {}. Expected 1.".format(len(files), fulldestpath))
    
    zipfilepath = os.path.join(fulldestpath, files[0])
    zipextractionpath = os.path.join(fulldestpath, "extracted")
    
    #Unzip the zipfile
    with zipfile.ZipFile(zipfilepath, 'r') as zip_ref:
        zip_ref.extractall(zipextractionpath) 
    
    extracteditems = os.listdir(zipextractionpath)
    if (len(extracteditems) != 1):
        raise Exception("{} directory expected 1 item but found {}".format(zipextractionpath, len(extracteditems)))    
    
    return os.path.join(zipextractionpath, extracteditems[0])    

def cleanup_s3(s3_bucket_name, s3_path):
    ''' Remove the successfully transferred data from the S3 bucket'''
    bucket = s3.Bucket(s3_bucket_name)
    if path_exists(s3_bucket_name, s3_path):
        resp = bucket.objects.filter(Prefix=s3_path).delete()
        
        if (len(resp) == 0):
            raise TransferException("Nothing was deleted for {}/{}".format(s3_bucket_name, s3_path))
        if ('Errors' in resp[0]):
            raise TransferException("Errors occurred while attempting deletion for {}/{}:\n{}".format(s3_bucket_name, s3_path, resp['Errors']))
    else:
        logging.warn("Prefix {}/{} does not exist".format(s3_bucket_name, s3_path))
    
        
        
