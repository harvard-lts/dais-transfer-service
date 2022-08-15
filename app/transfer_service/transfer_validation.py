import os, os.path, logging, hashlib, re, glob
from transfer_service.transferexception import ValidationException 

logfile=os.getenv('LOGFILE_PATH', 'hdc3a_transfer_service')
loglevel=os.getenv('LOGLEVEL', 'WARNING')
logging.basicConfig(filename=logfile, level=loglevel, format="%(asctime)s:%(levelname)s:%(message)s")

checksum_algorithm=os.getenv("CHECKSUM_ALGORITHM", "md5")

class ValidationReturnValue:
    def __init__(self, isvalid, errormessages):
        self.isvalid = isvalid
        self.errormessages = errormessages
        
    def isvalid(self):
        return self.isvalid
    
    def get_error_messages(self):
        return self.errormessages    

def validate_transfer(unzipped_data_direcory, data_directory_name):
    '''Validates the data that was transferred by checking for:
    1. The required files in the unzipped directory
    2. The hash mapping file against the actual files'''
    _does_data_exist(unzipped_data_direcory)
    retval = validate_required_unzipped_files(unzipped_data_direcory)
    #We only check for the hash mapping file if data exists
    if retval.isvalid and _does_data_exist(unzipped_data_direcory):
        retval = validate_mapping(unzipped_data_direcory)
            
    return retval

def validate_mapping(unzipped_data_direcory):
    '''Validates the data that was transferred by checking the 
    files against the provided hash mapping file'''
    
    mappingfilename = os.getenv("SUPPLIED_HASH_MAPPING_FILENAME")
    if mappingfilename is None:
        raise ValidationException("Missing env SUPPLIED_HASH_MAPPING_FILENAME")
    
    mappingpath = os.path.join(unzipped_data_direcory, mappingfilename)
    if not os.path.exists(mappingpath):
        raise ValidationException("Missing mapping file {}".format(mappingpath))
    
    isvalid = True
    incorrecthashes = []
    with open(mappingpath) as file:
        line_no = 1
        for line in file:
            hash_to_file = line.rstrip().split(" ", 1)
            if (len(hash_to_file) != 2):
                raise ValidationException("Incorrect formatting in mapping path {} in line number {}".format(mappingpath, line_no))
                
            provided_hashvalue = hash_to_file[0]
            filepath = os.path.join(unzipped_data_direcory, hash_to_file[1])
            calc_hashvalue = calculate_checksum(filepath)
                
            if provided_hashvalue != calc_hashvalue:
                incorrecthashes.append("File hash is incorrect for {}. Expected {} but received {}".format(filepath, provided_hashvalue, calc_hashvalue))
                logging.error("File hash is incorrect for {}. Expected {} but received {}".format(filepath, provided_hashvalue, calc_hashvalue))
                isvalid = False
                
            logging.debug(provided_hashvalue)
            logging.debug(filepath)
            logging.debug(calc_hashvalue)
            logging.debug("\n")
            line_no = line_no + 1
            
    retval = ValidationReturnValue(isvalid, incorrecthashes)
    
    return retval

                 
def validate_required_unzipped_files(unzipped_data_direcory):
    '''Validates that the list of required files provided in the .env
    exist once the zipfile is uncompressed.'''
    required_unzipped_files = os.getenv("REQUIRED_UNZIPPED_FILES", "")
    file_list = required_unzipped_files.rstrip().split(",")
    
    required_files_exist = True
    errormessages = []
    for file in file_list:
        filepath = os.path.join(unzipped_data_direcory,file)
        if not os.path.exists(filepath):
            required_files_exist = False
            msg = "Required file {} is missing from unzipped bag".format(filepath)
            errormessages.append(msg)
            logging.error(msg)
    
    retval = ValidationReturnValue(required_files_exist, errormessages)
    
    return retval

def _does_data_exist(unzipped_data_direcory):
    '''Checks whether any data exists'''
    data_files_exist=False
    data_dir = os.path.join(unzipped_data_direcory, "data")
    if os.path.exists(data_dir):
        files = glob.glob(data_dir + '/**/*.*', recursive=True)
        return len(files) > 0
    return False

def calculate_checksum(filepath):
    if checksum_algorithm == "sha256":
        return hashlib.sha256(open(filepath,'rb').read()).hexdigest()
    elif checksum_algorithm == "sha512":
        return hashlib.sha512(open(filepath,'rb').read()).hexdigest()
    #default to md5
    return hashlib.md5(open(filepath,'rb').read()).hexdigest()