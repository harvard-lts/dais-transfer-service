import json, time, traceback, stomp, sys, os, logging
import mqutils
import mqexception
import transfer_service, transfer_ready_validation

# Subscription id is unique to the subscription in this case there is only one subscription per connection
_sub_id = 1
_reconnect_attempts = 0
_max_attempts = 1000

logfile=os.getenv('LOGFILE_PATH', 'hdc3a_transfer_service')
loglevel=os.getenv('LOGLEVEL', 'WARNING')
logging.basicConfig(filename=logfile, level=loglevel)

def subscribe_to_listener(connection_params):
    logging.debug("************************ MQUTILS MQLISTENER - CONNECT_AND_SUBSCRIBE *******************************")
    global _reconnect_attempts
    _reconnect_attempts = _reconnect_attempts + 1
    if _reconnect_attempts <= _max_attempts:
        
        time.sleep(1)
        try:
            if not connection_params.conn.is_connected():
                connection_params.conn.connect(connection_params.user, connection_params.password, wait=True)
                logging.debug(f'subscribe_to_listener connecting {connection_params.queue} to with connection id 1 reconnect attempts: {_reconnect_attempts}')
            else:
                logging.debug(f'connect_and_subscibe already connected {connection_params.queue} to with connection id 1 reconnect attempts {_reconnect_attempts}')
        except Exception:
            logging.debug('Exception on disconnect. reconnecting...')
            logging.debug(traceback.format_exc())
            subscribe_to_listener(connection_params)
        else:
            if (connection_params.ack is not None):
                connection_params.conn.subscribe(destination=connection_params.queue, id=1, ack=connection_params.ack)
            else:
                connection_params.conn.subscribe(destination=connection_params.queue, id=1, ack='client-individual')
            _reconnect_attempts = 0
    else:
        logging.error('Maximum reconnect attempts reached for this connection. reconnect attempts: {}'.format(_reconnect_attempts))


class MqListener(stomp.ConnectionListener):
    def __init__(self, connection_params):
        self.connection_params = connection_params
        self.message_data = None
        self.message_id = None

    def on_error(self, frame):
        logging.debug('received an error "%s"' % frame.body)
        
    def on_message(self, frame):
        logging.debug("************************ MQUTILS MQLISTENER - ON_MESSAGE *******************************")
        headers, body = frame.headers, frame.body
        logging.debug('received a message headers "%s"' % headers)
        logging.debug('message body "%s"' % body)
        
        self.message_id = headers.get('message-id')
        try: 
            self.message_data = json.loads(body)
        except json.decoder.JSONDecodeError: 
            raise mqexception.MQException("Incorrect formatting of message detected.  Required JSON but received {} ".format(body))
        
        self.connection_params.conn.ack(self.message_id, 1)
        #TODO- Handle
        logging.debug(' message_data {}'.format(self.message_data))
        logging.debug(' message_id {}'.format(self.message_id))
        
        #Do not do the validation and transfer if dry_run is set
        if ("dry_run" in self.message_data):
            return 
        
        dropbox_dir = self.message_data['destination_path']
        
        try: 
            #Validate json
            transfer_ready_validation.validate_json_schema(self.message_data)
        
            #Transfer data
            logging.debug(' TRANSFERRING DATA {} to {}'.format(self.message_data['s3_path'], self.message_data['destination_path']))
            transfer_service.transfer_data(self.message_data)
        except Exception as e:
            transfer_status = mqutils.TransferStatus(self.message_data["package_id"], "failure", self.message_data['destination_path'])
            mqutils.notify_transfer_status_message(transfer_status)
            #TODO Send email message
            logging.exception("validation failed so transfer was not completed")
    

    def on_disconnected(self):
        logging.debug('disconnected! reconnecting...')
        subscribe_to_listener(self.connection_params)
        
    def get_connection(self):
        return self.connection_params.conn
    
    def get_message_data(self):
        return self.message_data
    
    def get_message_id(self):
        return self.message_id

         

def initialize_mqlistener():
    mqlistener = get_mqlistener()
    conn = mqlistener.get_connection()
    conn.set_listener('', mqlistener)
    subscribe_to_listener(mqlistener.connection_params)
    # http_clients://github.com/jasonrbriggs/stomp.py/issues/206
    while True:
        time.sleep(2)
        if not conn.is_connected():
            logging.debug('Disconnected in loop, reconnecting')
            subscribe_to_listener(mqlistener.connection_params)


def get_mqlistener(queue=None):
    connection_params = mqutils.get_transfer_mq_connection(queue)
    mqlistener = MqListener(connection_params)
    return mqlistener

    
if __name__ == "__main__":
    initialize_mqlistener()