import json
import logging
import sys
import time
from typing import Optional

import stomp
from stomp.utils import Frame

sys.path.append('app/mqresources')
import mqutils

logging.basicConfig(format='%(message)s')


def test_get_transfer_mq_connection():
    connection_params = mqutils.get_transfer_mq_connection()
    assert connection_params.conn is not None


def test_notification():
    '''Sends a status message to the transfer queue and verifies that it made it'''
    transfer_queue = "/queue/dropbox-transfer-status-testing"
    # Send the message
    transfer_status = mqutils.TransferStatus("12345", "success", "/path/to/dropbox",
                                             {"original_queue": transfer_queue, "retry_count": 0})
    message = mqutils.notify_transfer_status_message(transfer_status, transfer_queue)
    assert type(message) is str
    message_dict = json.loads(message)

    connection_params = mqutils.get_transfer_mq_connection(transfer_queue)

    mq_listener_object = TestConnectionListener()

    connection = connection_params.conn
    connection_params.conn.set_listener('', mq_listener_object)
    connection.subscribe(transfer_queue, id="test-connection")

    counter = 0
    # Try for 30 seconds then fail
    while mq_listener_object.get_message_body() is None:
        time.sleep(2)
        counter = counter + 2
        if counter >= 10:
            assert False, "test_notification: could not find anything on the {} after 30 seconds".format(transfer_queue)

    # dequeue the message
    message_body = mq_listener_object.get_message_body()
    assert message_body is not None
    assert type(message_body) is dict
    assert message_body == message_dict

    connection.disconnect()


class TestConnectionListener(stomp.ConnectionListener):
    def __init__(self) -> None:
        self.message_body = None

    def on_message(self, frame: Frame) -> None:
        self.message_body = json.loads(frame.body)

    def get_message_body(self) -> Optional[dict]:
        return self.message_body
