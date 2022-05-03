import os, json, stomp, logging, time

logfile=os.getenv('LOGFILE_PATH', 'drs_translation_service')
loglevel=os.getenv('LOGLEVEL', 'WARNING')
logging.basicConfig(filename=logfile, level=loglevel)

class ConnectionParams:
    def __init__(self, conn, queue, host, port, user, password, ack="client-individual"):
        self.conn = conn
        self.queue = queue
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.ack = ack
        
def get_transfer_mq_connection(queue=None):
    logging.debug("************************ MQUTILS - GET_TRANSFER_MQ_CONNECTION *******************************")
    try:
        host = os.getenv('TRANSFER_MQ_HOST')
        port = os.getenv('TRANSFER_MQ_PORT')
        user = os.getenv('TRANSFER_MQ_USER')
        password = os.getenv('TRANSFER_MQ_PASSWORD')
        if (queue is None):
            transfer_queue = os.getenv('TRANSFER_QUEUE_CONSUME_NAME')
        else:
            transfer_queue = queue
            
        conn = stomp.Connection([(host, port)], heartbeats=(40000, 40000), keepalive=True)
        conn.set_ssl([(host, port)])
        connection_params = ConnectionParams(conn, transfer_queue, host, port, user, password)
        conn.connect(user, password, wait=True)
    except Exception as e:
        logging.error(e)
        raise(e)
    return connection_params

def notify_transfer_status_message(queue=None):
    '''Creates a json message to notify the DIMS that the transfer has finished'''
    logging.debug("************************ MQUTILS - NOTIFY_TRANSFER_STATUS_MESSAGE *******************************")
    message = "No message"
    try:
        if (queue is None):
            transfer_queue = os.getenv('TRANSFER_QUEUE_PUBLISH_NAME')
        else:
            transfer_queue = queue
        
        #Add more details that will be needed from the load report.
        msg_json = {
            "package_id": "12345",
            "application_name": "Dataverse",
            "transfer_status": "success",
            "destination_path": "/path/to/dropbox",
            "admin_metadata": {"original_queue": transfer_queue}
        }

             
        #Default to one hour from now
        now_in_ms = int(time.time())*1000
        expiration = int(os.getenv('MESSAGE_EXPIRATION_MS', 36000000)) + now_in_ms
               
        logging.debug("msg json:")
        logging.debug(msg_json)
        message = json.dumps(msg_json)
        connection_params = get_transfer_mq_connection(transfer_queue)
        connection_params.conn.send(transfer_queue, message, headers = {"persistent": "true", "expires": expiration})
        logging.debug("MESSAGE TO QUEUE notify_transfer_status_message")
        logging.debug(message)
    except Exception as e:
        logging.error(e)
        raise(e)
    return message


