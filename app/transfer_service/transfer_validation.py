import os, os.path, logging, hashlib, re
from transfer_service.transferexception import ValidationException 

logfile=os.getenv('LOGFILE_PATH', 'hdc3a_transfer_service')
loglevel=os.getenv('LOGLEVEL', 'WARNING')
logging.basicConfig(filename=logfile, level=loglevel)

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
    1. The required files in the data directory
    2. The required files in the unzipped directory
    3. The hash mapping file against the actual files'''
    
    retval = validate_required_files(data_directory_name)
    if retval.isvalid:
        retval = validate_required_unzipped_files(unzipped_data_direcory)
    if retval.isvalid:
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
                
            print(provided_hashvalue)
            print(filepath)
            print(calc_hashvalue)
            print("\n")
            logging.debug(provided_hashvalue)
            logging.debug(filepath)
            logging.debug(calc_hashvalue)
            logging.debug("\n")
            line_no = line_no + 1
            
    retval = ValidationReturnValue(isvalid, incorrecthashes)
    
    return retval

def validate_required_files(data_directory_name):  
    '''Validates that there is DDI file (format of <doi>_datacite.v#.#xml)
    and a drsConfig file (format of drsConfig.<doi>_v#.#.json)'''
    ddiexists = False
    drsconfigexists = False
    for file in os.listdir(data_directory_name):
        if os.path.isfile(os.path.join(data_directory_name,file)):
            ddi_regexp = r"^{}_datacite\.v[0-9]+\.[0-9]+\.xml".format(os.path.basename(data_directory_name))
            drsconfig_regexp = r"^drsConfig\.{}_v[0-9]+\.[0-9]+\.json".format(os.path.basename(data_directory_name))
            if re.search(ddi_regexp, file):
                ddiexists = True
            elif re.search(drsconfig_regexp, file):
                drsconfigexists = True
    
    errormessages = []
    if not ddiexists:
        msg = "Missing DDI file.  Expected {}.datacite.v#.#.xml".format(os.path.basename(data_directory_name))
        errormessages.append(msg)
        logging.error(msg)
    if not drsconfigexists:
        msg = "Missing drsConfig file.  Expected drsConfig.{}_v#.#.xml".format(os.path.basename(data_directory_name))
        errormessages.append(msg)
        logging.error(msg)
    
    retval = ValidationReturnValue(ddiexists and drsconfigexists, errormessages)
    
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

def calculate_checksum(filepath):
    if checksum_algorithm == "sha256":
        return hashlib.sha256(open(filepath,'rb').read()).hexdigest()
    #default to md5
    return hashlib.md5(open(filepath,'rb').read()).hexdigest()