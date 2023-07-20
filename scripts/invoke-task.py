from celery import Celery
import os

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

s3_bucket = os.getenv("S3_BUCKET_NAME", "dataverse-export-dev")
s3_path = "doi-12-3456-transfer-service-test"
destination_path = os.path.join("/home/appuser/local/dropbox", s3_path)

transfer_task=os.getenv('TRANSFER_TASK_NAME', 'transfer_service.tasks.transfer_data')

arguments = {
            "dry_run": True,
            "application_name": "Dataverse",
            "package_id": "12345",
            "s3_path": s3_path,
            "s3_bucket_name": s3_bucket,
            "destination_path": destination_path,
            "admin_metadata": {"original_queue": os.getenv("TRANSFER_CONSUME_QUEUE_NAME"),
                               "task_name": transfer_task,
                               "retry_count": 0}}

res = app1.send_task(transfer_task,
                     args=[arguments], kwargs={},
                     queue=os.getenv("TRANSFER_CONSUME_QUEUE_NAME"))
