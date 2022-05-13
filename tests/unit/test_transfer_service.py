import pytest, sys, os.path, shutil
sys.path.append('app')
import transfer_service.transfer_validation as transfer_validation 
import transfer_service.transfer_service as transfer_service 

s3_path="doi-12-3456-transfer-service-test"
data_path = "/home/appuser/tests/data"

def test_unzip():
    dest_path = os.path.join(data_path, s3_path)
    zipextractionpath = transfer_service.unzip_transfer(dest_path)
    #Verify that the extracted directory now exists
    assert os.path.exists(zipextractionpath)

    cleanup_extraction(os.path.join(data_path, s3_path, "extracted"))

def cleanup_extraction(zipextractionpath):
    '''Removes the data that was downloaded into the dropbox'''
    try:
        shutil.rmtree(zipextractionpath)
    except OSError as e:
        print("Error: %s : %s" % (zipextractionpath, e.strerror))
        
def test_validate():
    dest_path = os.path.join(data_path, s3_path)
    zipextractionpath = transfer_service.unzip_transfer(dest_path)
    #Verify that the extracted directory now exists
    #Verify that the hash mapping file exists.
    assert os.path.exists(zipextractionpath)
    
    #Verify that the hash mapping file exists.
    assert os.path.exists(os.path.join(zipextractionpath, os.getenv("SUPPLIED_HASH_MAPPING_FILENAME")))
    
    assert transfer_validation.validate_transfer(zipextractionpath, os.path.join(data_path, s3_path))
    
    cleanup_extraction(os.path.join(data_path, s3_path, "extracted"))
