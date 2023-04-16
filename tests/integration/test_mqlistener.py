import json
import logging
import os
import os.path
import sys
import time, boto3
from collections import OrderedDict
from unittest.mock import patch

sys.path.append('app/transfer_service_mqresources')
sys.path.append('app/transfer_service')
import mqutils
from listener.transfer_ready_queue_listener import TransferReadyQueueListener
import transfer_helper

logging.basicConfig(format='%(message)s')

transfer_queue = "/queue/transfer-ready-testing-1"
transfer_queue2 = "/queue/transfer-ready-testing-2"
s3_bucket = os.getenv("S3_BUCKET_NAME", "dataverse-export-dev")
s3_path = "doi-12-3456-transfer-service-test"
destination_path = os.path.join("/home/appuser/local/dropbox", s3_path)


@patch("listener.transfer_ready_queue_listener.TransferReadyQueueListener._handle_received_message")
@patch("listener.transfer_ready_queue_listener.TransferReadyQueueListener._get_queue_name")
def test_listener(get_queue_name_mock, handle_received_message_mock):
    '''Tests to see if the listener picks up a message from the queue'''
    get_queue_name_mock.return_value = transfer_queue
    mq_listener_object = TransferReadyQueueListener()

    # This call only puts a dry run message on the queue so it will not validate and transfer
    message_json = notify_dry_run_data_ready_transfer_message()

    counter = 0
    # Try for 30 seconds then fail
    while not handle_received_message_mock.call_count:
        time.sleep(2)
        counter = counter + 2
        if counter >= 10:
            assert False, "test_listener: could not find anything on the {} after 30 seconds".format(transfer_queue)

    args, kwargs = handle_received_message_mock.call_args
    assert type(args[0]) is dict
    assert OrderedDict(args[0]) == OrderedDict(message_json)

    # cleanup the queue and disconnect the listener
    mq_listener_object._acknowledge_message(args[1], args[2])
    mq_listener_object.disconnect()


def notify_dry_run_data_ready_transfer_message():
    '''Creates a dummy queue json message to notify the queue that the 
    DVN data is ready to be transferred but does not actually do the transfer.  '''
    try:

        message_json = {
            "dry_run": True,
            "application_name": "Dataverse",
            "package_id": "12345",
            "s3_path": s3_path,
            "s3_bucket_name": s3_bucket,
            "destination_path": destination_path,
            "admin_metadata": {"original_queue": transfer_queue, "retry_count": 0}
        }

        print("msg json:")
        print(message_json)
        message = json.dumps(message_json)
        connection_params = mqutils.get_transfer_mq_connection(transfer_queue)
        # Default to one hour from now
        now_in_ms = int(time.time()) * 1000
        expiration = int(os.getenv('MESSAGE_EXPIRATION_MS', 36000000)) + now_in_ms
        print("Expiration: {}".format(expiration))

        connection_params.conn.send(transfer_queue, message, headers={"persistent": "true", "expires": expiration})
        print("MESSAGE TO QUEUE {} is {}".format(transfer_queue, message))
    except Exception as e:
        print(e)
        raise e
    return message_json
