import logging, os, os.path, boto3, sys, pytest
sys.path.append('app/transfer_service')
import transfer_service.transfer_service as transfer_service
import transfer_helper
from transfer_service.transferexception import TransferException 
from botocore.client import ClientError

logging.basicConfig(format='%(message)s')

s3_path="doi-12-3456-transfer-service-test"
    
def test_perform_dvn_transfer():
    '''Tests to see if data can be transferred to the dropbox'''
    s3 = boto3.resource('s3',
                aws_access_key_id=os.getenv("DVN_AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("DVN_AWS_SECRET_ACCESS_KEY"),
                region_name="us-east-1")
      
    s3_bucket="dataverse-export-dev"
    #Upload the data to s3 to test the dropbox transfer
    transfer_helper.upload_sample_data(s3, s3_bucket, s3_path)
       
    assert transfer_service.path_exists(s3, s3_bucket, s3_path)
       
    dropbox_path="/home/appuser/local/dropbox"
    dest_path = os.path.join(dropbox_path, os.path.basename(s3_path))
       
    transfer_service.perform_transfer(s3, s3_bucket, s3_path, dest_path)
       
    assert os.path.exists(dest_path)
       
    #cleanup the data that was moved to the dropbox
    transfer_helper.cleanup_dropbox(dest_path)
     
def test_perform_epadd_transfer():
    '''Tests to see if data can be transferred to the dropbox'''
      
    s3 = boto3.resource('s3',
            aws_access_key_id=os.getenv("EPADD_AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("EPADD_AWS_SECRET_ACCESS_KEY"),
            region_name="us-east-1")
     
    s3_bucket="epadd-export-dev" 
    #Upload the data to s3 to test the dropbox transfer
    transfer_helper.upload_sample_data(s3, s3_bucket, s3_path)
       
    assert transfer_service.path_exists(s3, s3_bucket, s3_path)
       
    dropbox_path="/home/appuser/local/dropbox"
    dest_path = os.path.join(dropbox_path, os.path.basename(s3_path))
       
    transfer_service.perform_transfer(s3, s3_bucket, s3_path, dest_path)
       
    assert os.path.exists(dest_path)
       
    #cleanup the data that was moved to the dropbox
    transfer_helper.cleanup_dropbox(dest_path)
     
def test_dvn_cleanup_s3():
    '''Tests to make sure the s3 cleanup method works'''
    #Upload the data to s3 to test the dropbox transfer
    s3 = boto3.resource('s3',
            aws_access_key_id=os.getenv("DVN_AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("DVN_AWS_SECRET_ACCESS_KEY"),
            region_name="us-east-1")
    
    s3_bucket="dataverse-export-dev"
    transfer_helper.upload_sample_data(s3, s3_bucket, s3_path)
     
    assert transfer_service.path_exists(s3, s3_bucket, s3_path)
     
    transfer_service.cleanup_s3(s3, s3_bucket, s3_path)
     
    assert not transfer_service.path_exists(s3, s3_bucket, s3_path)
     
def test_epadd_cleanup_s3():
    '''Tests to make sure the s3 cleanup method works'''
     
    s3 = boto3.resource('s3',
            aws_access_key_id=os.getenv("EPADD_AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("EPADD_AWS_SECRET_ACCESS_KEY"),
            region_name="us-east-1")
     
    s3_bucket="epadd-export-dev" 
    #Upload the data to s3 to test the dropbox transfer
    transfer_helper.upload_sample_data(s3, s3_bucket, s3_path)
     
    assert transfer_service.path_exists(s3, s3_bucket, s3_path)
     
    transfer_service.cleanup_s3(s3, s3_bucket, s3_path)
     
    assert not transfer_service.path_exists(s3, s3_bucket, s3_path)
     
def test_cleanup_s3_failure():
    with pytest.raises(ClientError):
        '''Tests to make sure the s3 cleanup method fails when the bucket doesn't exists'''
         
        s3 = boto3.resource('s3',
            aws_access_key_id=os.getenv("DVN_AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("DVN_AWS_SECRET_ACCESS_KEY"),
            region_name="us-east-1")
        transfer_service.cleanup_s3(s3, "non-existant-bucket", "non-existant-path")


