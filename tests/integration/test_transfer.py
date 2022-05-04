import logging, os, os.path, boto3, sys
sys.path.append('app/transfer_service')
import transfer_service.transfer_service as transfer_service
import transfer_helper

logging.basicConfig(format='%(message)s')

s3_bucket=os.getenv("S3_BUCKET_NAME", "dataverse-export-dev")
s3_path="doi-12-3456-transfer-service-test"
    
def test_perform_transfer():
    '''Tests to see if data can be transferred to the dropbox'''
    #Upload the data to s3 to test the dropbox transfer
    transfer_helper.upload_sample_data(s3_bucket, s3_path)
    
    dropbox_path="/home/appuser/local/dropbox"
    
    dest_path = os.path.join(dropbox_path, s3_path)
    
    transfer_service.perform_transfer(s3_bucket, s3_path, dest_path)
    
    assert os.path.exists(dest_path)
    
    #cleanup the data that was moved to the dropbox
    transfer_helper.cleanup_dropbox(dest_path)
    




