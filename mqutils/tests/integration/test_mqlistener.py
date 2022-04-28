import sys, os, logging, time, json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import mqutils as mqutils
import mqlistener as mqlistener

logging.basicConfig(format='%(message)s')

_transfer_queue = "/queue/transfer-ready-testing"


def test_listener():
    '''Tests to see if the listener picks up a message from the queue'''
    mqlistenerobject = mqlistener.get_mqlistener(_transfer_queue)
    
    conn = mqlistenerobject.get_connection()
    conn.set_listener('', mqlistenerobject)
    mqlistener.subscribe_to_listener(mqlistenerobject.connection_params)
    
    message = notify_data_ready_transfer_message()
    messagedict = json.loads(message)
    
    counter = 0
    #Try for 30 seconds then fail
    while mqlistenerobject.get_message_data() is None:
        time.sleep(2)
        counter = counter+2
        if not conn.is_connected():
            mqlistener.subscribe_to_listener(mqlistenerobject.connection_params)
        if counter >= 10:
            assert False, "test_listener: could not find anything on the {} after 30 seconds".format(_transfer_queue)
        
    assert mqlistenerobject.get_message_data() is not None
    assert type(mqlistenerobject.get_message_data()) is dict
    assert mqlistenerobject.get_message_data() == messagedict
    

       
def notify_data_ready_transfer_message():
    '''Creates a dummy queue json message to notify the queue that the 
    DVN data is ready to be transferred.  '''
    message = "No message"
    try:
        
        msg_json = {
            "package_id": "12345",
            "s3_path": "/path/to/data",
            "s3_bucket_name": "dataverse-export-dev",
            "destination_path": "/path/to/dropbox",
            "admin_metadata": {"original_queue": _transfer_queue}
        }


        print("msg json:")
        print(msg_json)
        message = json.dumps(msg_json)
        connection_params = mqutils.get_transfer_mq_connection(_transfer_queue)
        #Default to one hour from now
        now_in_ms = int(time.time())*1000
        expiration = int(os.getenv('MESSAGE_EXPIRATION_MS', 36000000)) + now_in_ms
        print("Expiration: {}".format(expiration))
        
        connection_params.conn.send(_transfer_queue, message, headers = {"persistent": "true", "expires": expiration})
        print("MESSAGE TO QUEUE {} is {}".format(_transfer_queue, message))
    except Exception as e:
        print(e)
        raise(e)
    return message


