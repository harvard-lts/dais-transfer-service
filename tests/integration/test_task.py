from celery import Celery
from kombu import Queue
import os

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

def test_publish_queue_task():
    '''Verifies that tasks can be published to a queue
    that this celery worker does not consume'''
    package_id = "12345"
    transfer_status_task = os.getenv('TRANSFER_STATUS_TASK_NAME', 'dims.tasks.handle_transfer_status')
    msg_json = {
        "dlq_testing": True,
        "package_id": package_id,
        "transfer_status": "success",
        "destination_path": "/test/destination/path",
        "admin_metadata": {
            "original_queue": os.getenv("TRANSFER_PUBLISH_QUEUE_NAME"),
            "task_name": transfer_status_task,
            "retry_count": 0
        }
    }
    myqueue = Queue(
        os.getenv("TRANSFER_PUBLISH_QUEUE_NAME"), no_declare=True)
    try:
        app1.send_task(transfer_status_task, args=[msg_json], kwargs={},
            queue=myqueue)
        assert True
    except:
        assert False
