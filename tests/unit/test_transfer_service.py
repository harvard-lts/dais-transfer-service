import pytest, sys, os.path, shutil
sys.path.append('app')
import transfer_service.transfer_validation as transfer_validation 
import transfer_service.transfer_service as transfer_service 

s3_path="doi-12-3456-transfer-service-test"
data_path = "/home/appuser/tests/data"

def test_unzip():
    zipextractionpath = transfer_service.unzip_transfer(data_path, s3_path)
    #Verify that the extracted directory now exists
    #Verify that the hash mapping file exists.
    assert os.path.exists(zipextractionpath)
    
    #Verify that the hash mapping file exists.
    assert os.path.exists(os.path.join(zipextractionpath, os.getenv("SUPPLIED_HASH_MAPPING_FILENAME")))
    
    cleanup_extraction(os.path.join(data_path, s3_path, "extracted"))

def cleanup_extraction(zipextractionpath):
    '''Removes the data that was downloaded into the dropbox'''
    try:
        shutil.rmtree(zipextractionpath)
    except OSError as e:
        print("Error: %s : %s" % (zipextractionpath, e.strerror))