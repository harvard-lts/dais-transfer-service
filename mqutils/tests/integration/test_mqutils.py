import sys, os, logging, time, json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import mqutils as mqutils
import mqlistener as mqlistener

logging.basicConfig(format='%(message)s')

def test_get_transfer_mq_connection():
    connection_params = mqutils.get_transfer_mq_connection()
    assert connection_params.conn is not None

def test_notification():
    '''Sends a status message to the transfer queue and verifies that it made it'''
    transfer_queue="/queue/dropbox-transfer-status-testing"
    #Send the message
    message = mqutils.notify_transfer_status_message(transfer_queue)
    assert type(message) is str
    messagedict = json.loads(message)
    
    mqlistenerobject = mqlistener.get_mqlistener(transfer_queue)
     
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
