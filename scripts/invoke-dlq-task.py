from celery import Celery
import os

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

transfer_task=os.getenv('TRANSFER_TASK_NAME', 'transfer_service.tasks.transfer_data')

arguments = {
            "dlq_testing": True,
            "package_id": "doi-testing"
            }

res = app1.send_task(transfer_task,
                     args=[arguments], kwargs={},
                     queue=os.getenv("TRANSFER_CONSUME_QUEUE_NAME"))
