import boto3, os, os.path, logging, zipfile, glob
import shutil
from botocore.exceptions import ClientError
import transfer_service.transfer_ready_validation as transfer_ready_validation
from transfer_service.transferexception import TransferException 
from transfer_service.transferexception import ValidationException 
import transfer_service.transfer_validation as transfer_validation 
from celery import Celery

app = Celery()
app.config_from_object('celeryconfig')

logger = logging.getLogger('transfer-service')
def transfer_data(message_data):
    s3 = None
    s3_client = None
    application_name = ""
    if ('application_name' in message_data):
        application_name = message_data['application_name']
        if (message_data['application_name'] == "Dataverse"):
            s3 = boto3.resource('s3',
                aws_access_key_id=os.getenv("DVN_AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("DVN_AWS_SECRET_ACCESS_KEY"),
                region_name="us-east-1")
        elif (message_data['application_name'] == "ePADD"):
            # TODO: Make creation of s3_client configurable to make adding non-Amazon S3 implementations more flexible
            # TODO: Refactor code to use only boto client instead of boto3 resource
            # JIRA Ticket: https://jira.huit.harvard.edu/browse/LTSEPADD-28
            s3 = boto3.resource('s3',
                aws_access_key_id=os.getenv("EPADD_AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("EPADD_AWS_SECRET_ACCESS_KEY"),
                region_name="us-east-1")
            s3_client = boto3.client('s3',
                aws_access_key_id=os.getenv("EPADD_AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("EPADD_AWS_SECRET_ACCESS_KEY"),
                region_name="us-east-1")
    else:
         raise TransferException("The application_name parameter does not exist in the message body {}.".format(message_data))  
               
    s3_bucket_name = message_data['s3_bucket_name']
    s3_path = message_data['s3_path'] 
    fs_source_path = message_data['fs_source_path'] 
    dest_path = os.path.join(message_data['destination_path'], message_data['package_id'])
   
    error_message = ""
    if not path_exists(s3, s3_bucket_name, s3_path) and not os.path.exists(fs_source_path):
        error_message = "The path {} does not exists in bucket {}.".format(s3_path, s3_bucket_name)
        error_message += "and The FS path {} does not exist. Either an S3 or FS path must be provided".format(fs_source_path)    
        raise TransferException(error_message)  

    if path_exists(s3, s3_bucket_name, s3_path):
        #Transfer
        perform_transfer(s3, s3_bucket_name, s3_path, dest_path)
    elif os.path.exists(fs_source_path): 
        perform_fs_transfer(os.path.basename(fs_source_path), os.path.dirname(fs_source_path), dest_path)

    zipextractionpath = ""
    if (application_name == "Dataverse"):
        zipextractionpath = unzip_transfer(dest_path)

        #Type ValidationReturnValue
        validation_retval : transfer_validation.ValidationReturnValue  = transfer_validation.validate_transfer(zipextractionpath, dest_path)
        #Validate transfer
        if not validation_retval.isvalid:
            msg = "Transfer Validation Failed"
            msg = msg + "\n" + ','.join(validation_retval.get_error_messages())
            raise ValidationException(msg)


#     elif application_name == "ePADD":
# 
#         try:
#             # Type ValidationReturnValue
#             validation_retval: transfer_validation.ValidationReturnValue = transfer_validation.validate_zipped_transfer(s3_client, message_data)
#             # Validate transfer
#             if not validation_retval.isvalid:
#                 msg = "Transfer Validation Failed Gracefully:"
#                 msg = msg + "\n" + ','.join(validation_retval.get_error_messages())
#                 logging.error(msg)
#                 raise ValidationException(msg)
#         except ValidationException as e:
#             logging.exception("Transfer Validation Failed with Exception {}".format(str(e)))
#             raise e

    package_id = message_data["package_id"]
    transfer_status_task = os.getenv('TRANSFER_STATUS_TASK_NAME', 'dims.tasks.handle_transfer_status')
    msg_json = {
        "package_id": package_id,
        "transfer_status": "success",
        "destination_path": dest_path,
        "admin_metadata": {
            "original_queue": os.getenv("TRANSFER_PUBLISH_QUEUE_NAME"),
            "task_name": transfer_status_task,
            "retry_count": 0
        }
    }
    app.send_task(transfer_status_task, args=[msg_json], kwargs={},
            queue=os.getenv("TRANSFER_PUBLISH_QUEUE_NAME")) 
            
    #Cleanup s3
    cleanup_s3(s3, s3_bucket_name, s3_path)

def path_exists(s3, s3_bucket, s3_path):
    if not s3_bucket or not s3_path:
        return False
    try:
        bucket = s3.Bucket(s3_bucket)
        res = bucket.objects.filter(Prefix=s3_path)
        if (list(res.limit(1))):
            return True
        
        return False
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e
       
def perform_transfer(s3, s3_bucket_name, s3_path, dropbox_dir):
    """
    Download the contents of a folder directory
    Args:
        s3: The client with credentials
        s3_bucket_name: the name of the s3 bucket
        s3_path: the folder path in the s3 bucket
        dropbox_dir: an absolute directory path in the local file system
    """
    
    logger.debug("Transferring {}/{} to {}".format(s3_bucket_name, s3_path, dropbox_dir))
        
    bucket = s3.Bucket(s3_bucket_name)
    for obj in bucket.objects.filter(Prefix=s3_path):
        target = os.path.join(dropbox_dir, os.path.relpath(obj.key, s3_path))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == '/':
            continue
        logger.debug("Downloading {} to {}".format(obj.key, target))
        bucket.download_file(obj.key, target)

       
def perform_fs_transfer(file_name, file_path, dropbox_dir):
    """
    Copy the contents of a folder directory
    Args:
        file_name: the name of the file
        file_path: the folder path in the s3 bucket
        dropbox_dir: an absolute directory path in the local file system
    """
    
    logger.debug("Transferring {}/{} to {}".format(file_name, file_path, dropbox_dir))

    source = os.path.join(file_path, file_name)    
    target = os.path.join(dropbox_dir, file_name)
    if not os.path.exists(os.path.dirname(target)):
        os.makedirs(os.path.dirname(target))
    logger.debug("Copying {} to {}".format(source, target))
    shutil.copy(source, target)

def unzip_transfer(fulldestpath):
    '''Unzips the transferred zipfile'''

    zip_file_re = r"{}/*.zip".format(fulldestpath)
    files = glob.glob(zip_file_re)
    if len(files) == 0:
        raise TransferException("No zip files found in {}".format(fulldestpath))
    elif len(files) > 1:
        raise TransferException("{} zip files found in {}. Expected 1.".format(len(files), fulldestpath))
    
    zipfilepath = os.path.join(fulldestpath, files[0])
    zipextractionpath = os.path.join(fulldestpath, "extracted")
    
    #Unzip the zipfile
    with zipfile.ZipFile(zipfilepath, 'r') as zip_ref:
        zip_ref.extractall(zipextractionpath) 
    
    extracteditems = os.listdir(zipextractionpath)
    if (len(extracteditems) != 1):
        raise TransferException("{} directory expected 1 item but found {}".format(zipextractionpath, len(extracteditems)))    
    
    return os.path.join(zipextractionpath, extracteditems[0])    

def cleanup_s3(s3, s3_bucket_name, s3_path):
    ''' Remove the successfully transferred data from the S3 bucket'''
    if path_exists(s3, s3_bucket_name, s3_path):
        bucket = s3.Bucket(s3_bucket_name)
        resp = bucket.objects.filter(Prefix=s3_path).delete()
        if (len(resp) == 0):
            raise TransferException("Nothing was deleted for {}/{}".format(s3_bucket_name, s3_path))
        if ('Errors' in resp[0]):
            raise TransferException("Errors occurred while attempting deletion for {}/{}:\n{}".format(s3_bucket_name, s3_path, resp['Errors']))
    else:
        logger.warn("Prefix {}/{} does not exist".format(s3_bucket_name, s3_path))     
