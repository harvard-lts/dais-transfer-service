import sys, os, logging, time, json, stomp
sys.path.append('app/mqresources')
import mqutils
import mqlistener

logging.basicConfig(format='%(message)s')

def test_get_transfer_mq_connection():
    connection_params = mqutils.get_transfer_mq_connection()
    assert connection_params.conn is not None

def test_notification():
    '''Sends a status message to the transfer queue and verifies that it made it'''
    transfer_queue="/queue/dropbox-transfer-status-testing"
    #Send the message
    transfer_status = mqutils.TransferStatus("12345", "Dataverse", "success", "/path/to/dropbox", {"original_queue" : transfer_queue, "retry_count":0})
    message = mqutils.notify_transfer_status_message(transfer_status,transfer_queue)
    assert type(message) is str
    messagedict = json.loads(message)
    
    connection_params = mqutils.get_transfer_mq_connection(transfer_queue)
    mqlistenerobject = HelperMqListener(connection_params)
      
    conn = mqlistenerobject.get_connection()
    conn.set_listener('', mqlistenerobject)
    mqlistener.subscribe_to_listener(mqlistenerobject.connection_params)
     
    counter = 0
    #Try for 30 seconds then fail
    while mqlistenerobject.get_message_data() is None:
        time.sleep(2)
        counter = counter+2
        if not conn.is_connected():
            mqlistener.subscribe_to_listener(mqlistenerobject.connection_params)
        if counter >= 10:
            assert False, "test_notification: could not find anything on the {} after 30 seconds".format(transfer_queue)
    #dequeue the message
    conn.ack(mqlistenerobject.get_message_id(), 1)    
    assert mqlistenerobject.get_message_data() is not None
    assert type(mqlistenerobject.get_message_data()) is dict
    assert mqlistenerobject.get_message_data() == messagedict
    

class HelperMqListener(stomp.ConnectionListener):
    '''A helper listener to consume the transfer status message placed on the queue'''
    def __init__(self, connection_params):
        self.connection_params = connection_params
        self.message_data = None
        self.message_id = None
    def on_error(self, frame):
        logging.debug('received an error "%s"' % frame.body)
        print('received an error "%s"' % frame.body)

    def on_message(self, frame):
        headers, body = frame.headers, frame.body
        self.message_id = headers.get('message-id')
        try: 
            self.message_data = json.loads(body)
        except json.decoder.JSONDecodeError: 
            raise mqexception.MQException("Incorrect formatting of message detected.  Required JSON but received {} ".format(body))
        
        self.connection_params.conn.ack(self.message_id, 1)

    def on_disconnected(self):
        logging.debug('disconnected! reconnecting...')
        subscribe_to_listener(self.connection_params)
        
    def get_connection(self):
        return self.connection_params.conn
    
    def get_message_data(self):
        return self.message_data
    
    def get_message_id(self):
        return self.message_id
