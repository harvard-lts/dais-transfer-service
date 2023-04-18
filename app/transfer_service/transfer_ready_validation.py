import os, os.path, json, jsonschema, logging
from transfer_service.transferexception import ValidationException

logger = logging.getLogger('transfer-service')
def validate_json_schema(json_data):

    app_path = os.path.abspath(os.path.dirname(__file__))
    schemasdir = os.path.join(app_path, "schemas")
    
    logger.debug("Schemas dir: {}".format(schemasdir))
    
    if json_data is None:
        raise ValidationException("Missing JSON data in validate_json_schema")
        
    try:
        with open('{}/{}.json'.format(schemasdir, 'transfer_ready')) as json_file:
            json_model = json.load(json_file)
    except Exception as e:
       raise ValidationException("Unable to get json schema model.") from e
        
    try:
        jsonschema.validate(json_data, json_model)
    except json.decoder.JSONDecodeError as e:
        raise ValidationException("Invalid JSON format") from e
    except jsonschema.exceptions.ValidationError as e:
        raise ValidationException("Invalid JSON schema") from e

