import jsonschema, json, pytest, sys
sys.path.append('app')
import transfer_service.transfer_ready_validation as transfer_ready_validation
from transfer_service.transferexception import ValidationException

def test_valid_json():
    msg_json = {
        "package_id": "12345",
        "application_name": "Dataverse",
        "s3_path": "/path/to/data",
        "s3_bucket_name": "dataverse-export-dev",
        "fs_source_path": "",
        "destination_path": "/home/appuser/dropbox",
        "admin_metadata": {"original_queue": "myqueue", "retry_count":0}
    }
    
    try: 
        transfer_ready_validation.validate_json_schema(msg_json)
        assert True
    except Exception:
        assert False

def test_valid_json_extra_admin_params():
    msg_json = {
        "package_id": "12345",
        "application_name": "Dataverse",
        "s3_path": "/path/to/data",
        "s3_bucket_name": "dataverse-export-dev",
        "fs_source_path": "",
        "destination_path": "/home/appuser/dropbox",
        "admin_metadata": {"original_queue": "myqueue", "retry_count":0, "extra_admin_param": "should be valid"}
    }
    
    try: 
        transfer_ready_validation.validate_json_schema(msg_json)
        assert True
    except Exception:
        assert False
    
def test_invalid_json_missing_param():
    with pytest.raises(ValidationException):
        msg_json = {
            "package_id": "12345",
            "application_name": "Dataverse",
            "s3_bucket_name": "dataverse-export-dev",
            "destination_path": "/home/appuser/dropbox",
            "admin_metadata": {"original_queue": "myqueue", "retry_count":0}
        }
    
        transfer_ready_validation.validate_json_schema(msg_json)
