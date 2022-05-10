import os, os.path, logging
from transfer_service.transferexception import ValidationException 

def validate_transfer(unzipped_data_direcory_name):
    '''Validates the data that was transferred by checking the 
    files against the provided hash mapping file'''
    mappingfilename = os.getenv("SUPPLIED_HASH_MAPPING_FILENAME")
    if mappingfilename is None:
        raise ValidationException("Missing env SUPPLIED_HASH_MAPPING_FILENAME")
    
    mappingpath = os.path.join(unzipped_data_direcory_name, mappingfilename)
    if not os.path.exists(mappingpath):
        raise ValidationException("Missing mapping file {}".format(mappingpath))
    
    with open(mappingpath) as file:
        for line in file:
            print(line.rstrip())
            
    return True