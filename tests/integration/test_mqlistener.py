import sys, os, os.path, logging, time, json
sys.path.append('app/mqresources')
sys.path.append('app/transfer_service')
import mqutils
import mqlistener
import transfer_helper

logging.basicConfig(format='%(message)s')

transfer_queue = "/queue/transfer-ready-testing-1"
transfer_queue2 = "/queue/transfer-ready-testing-2"
s3_bucket=os.getenv("S3_BUCKET_NAME", "dataverse-export-dev")
s3_path="doi-12-3456-transfer-service-test"
destination_path=os.path.join("/home/appuser/local/dropbox",s3_path)

def test_listener():
    '''Tests to see if the listener picks up a message from the queue'''
    mqlistenerobject = mqlistener.get_mqlistener(transfer_queue)
    
    conn = mqlistenerobject.get_connection()
    conn.set_listener('', mqlistenerobject)
    mqlistener.subscribe_to_listener(mqlistenerobject.connection_params)
    
    #This call only puts a dry run message on the queue so it will not validate and transfer
    message = notify_dry_run_data_ready_transfer_message()
    messagedict = json.loads(message)
    
    counter = 0
    #Try for 30 seconds then fail
    while mqlistenerobject.get_message_data() is None:
        time.sleep(2)
        counter = counter+2
        if not conn.is_connected():
            mqlistener.subscribe_to_listener(mqlistenerobject.connection_params)
        if counter >= 10:
            assert False, "test_listener: could not find anything on the {} after 30 seconds".format(transfer_queue)
         
    assert mqlistenerobject.get_message_data() is not None
    assert type(mqlistenerobject.get_message_data()) is dict
    assert mqlistenerobject.get_message_data() == messagedict
    
    mqlistenerobject.connection_params.conn.unsubscribe(1)
   
def test_listener_and_transfer():
    '''Tests to see if the listener picks up a message from the queue and triggers the transfer'''
    #Upload the data to s3 to test the dropbox transfer
    transfer_helper.upload_sample_data(s3_bucket, s3_path)
    
    mqlistenerobject = mqlistener.get_mqlistener(transfer_queue2)
    
    conn = mqlistenerobject.get_connection()
    conn.set_listener('', mqlistenerobject)
    mqlistener.subscribe_to_listener(mqlistenerobject.connection_params)
    
    #This call puts a real message on the queue so it should also do the transfer.
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
            assert False, "test_listener: could not find anything on the {} after 30 seconds".format(transfer_queue2)
         
    assert mqlistenerobject.get_message_data() is not None
    assert type(mqlistenerobject.get_message_data()) is dict
    assert mqlistenerobject.get_message_data() == messagedict 
    assert os.path.exists(destination_path)
    
    #cleanup the data that was moved to the dropbox
    transfer_helper.cleanup_dropbox(destination_path)
    mqlistenerobject.connection_params.conn.unsubscribe(1)
    

       
def notify_dry_run_data_ready_transfer_message():
    '''Creates a dummy queue json message to notify the queue that the 
    DVN data is ready to be transferred but does not actually do the transfer.  '''
    try:
        
        msg_json = {
            "dry_run": True,
            "application_name": "Dataverse",
            "package_id": "12345",
            "s3_path": s3_path,
            "s3_bucket_name": s3_bucket,
            "destination_path": destination_path,
            "admin_metadata": {"original_queue": transfer_queue, "retry_count":0}
        }

        print("msg json:")
        print(msg_json)
        message = json.dumps(msg_json)
        connection_params = mqutils.get_transfer_mq_connection(transfer_queue)
        #Default to one hour from now
        now_in_ms = int(time.time())*1000
        expiration = int(os.getenv('MESSAGE_EXPIRATION_MS', 36000000)) + now_in_ms
        print("Expiration: {}".format(expiration))
        
        connection_params.conn.send(transfer_queue, message, headers = {"persistent": "true", "expires": expiration})
        print("MESSAGE TO QUEUE {} is {}".format(transfer_queue, message))
    except Exception as e:
        print(e)
        raise(e)
    return message

def notify_data_ready_transfer_message():
    '''Creates a dummy queue json message to notify the queue that the 
    DVN data is ready to be transferred but does not actually do the transfer.  '''
    try:
        
        msg_json = {
            "package_id": "doi-12-3456-transfer-service-test",
            "application_name": "Dataverse",
            "s3_path": s3_path,
            "s3_bucket_name": s3_bucket,
            "destination_path": destination_path,
            "admin_metadata": {"original_queue": transfer_queue2, "retry_count":0}
        }

        print("msg json:")
        print(msg_json)
        message = json.dumps(msg_json)
        connection_params = mqutils.get_transfer_mq_connection(transfer_queue2)
        #Default to one hour from now
        now_in_ms = int(time.time())*1000
        expiration = int(os.getenv('MESSAGE_EXPIRATION_MS', 36000000)) + now_in_ms
        print("Expiration: {}".format(expiration))
        
        connection_params.conn.send(transfer_queue2, message, headers = {"persistent": "true", "expires": expiration})
        print("MESSAGE TO QUEUE {} is {}".format(transfer_queue2, message))
    except Exception as e:
        print(e)
        raise(e)
    return message



