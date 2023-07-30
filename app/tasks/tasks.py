from celery import Celery
import os
import traceback
import transfer_service.transfer_ready_validation as transfer_ready_validation
import transfer_service.transfer_service as transfer_service
import notifier.notifier as notifier
from transfer_service.transferexception import ValidationException
from transfer_service.transferexception import TransferException
import logging

app = Celery()
app.config_from_object('celeryconfig')

logger = logging.getLogger('transfer-service')

transfer_task = os.getenv('TRANSFER_TASK_NAME', 'transfer_service.tasks.transfer_data')
transfer_status_task = os.getenv('TRANSFER_STATUS_TASK_NAME', 'dims.tasks.handle_transfer_status')
retries = int(os.getenv('MESSAGE_MAX_RETRIES', 3))

@app.task(bind=True, serializer='json', name=transfer_task, max_retries=retries, acks_late=True, autoretry_for=(Exception,))
def transfer_data(self, message_body):
    logger.debug("Message Body: {}".format(message_body))
    # Do not do the validation and transfer if dry_run is set
    if "dry_run" in message_body:
        app.send_task("tasks.tasks.do_task", args=[{"dryrun": "for transfer_data"}], kwargs={},
            queue=os.getenv("TRANSFER_PUBLISH_QUEUE_NAME") + "-dryrun")
        return

    
    try:
        # Validate json
        transfer_ready_validation.validate_json_schema(message_body)

        # Transfer data
        logger.debug(
            'TRANSFERRING DATA {} to {}'.format(message_body['s3_path'], message_body['destination_path'])
        )
        transfer_service.transfer_data(message_body)
    except ValidationException as e:
        msg = "Validation failed so transfer was not completed"
        exception_msg = traceback.format_exc()
        failureEmail = message_body["admin_metadata"]["failureEmail"]
        exception_msg = traceback.format_exc()
        send_error_notifications(msg, message_body, e, exception_msg, failureEmail, self.request.retries)
    except TransferException as e:
        msg = str(e)
        exception_msg = traceback.format_exc()
        failureEmail = message_body["admin_metadata"]["failureEmail"]
        exception_msg = traceback.format_exc()
        send_error_notifications(msg, message_body, e, exception_msg, failureEmail, self.request.retries)
    except Exception as e:
        msg = str(e)
        exception_msg = traceback.format_exc()
        body = msg + "\n" + exception_msg
        send_error_notifications(msg, message_body, e, exception_msg, None, self.request.retries)

def send_error_notifications(message_start, message_body, exception, exception_msg, emails=None, num_retries=0):
    package_id = message_body.get("package_id")
    msg_json = {
        "package_id": package_id,
        "transfer_status": "failure",
        "destination_path": message_body.get('destination_path'),
        "admin_metadata": {
            "original_queue": os.getenv("TRANSFER_PUBLISH_QUEUE_NAME"),
            "task_name": transfer_task,
            "retry_count": 0
        }
    }
    app.send_task(transfer_status_task, args=[msg_json], kwargs={},
            queue=os.getenv("TRANSFER_PUBLISH_QUEUE_NAME"))
    body = message_start + "\n" + exception_msg
    notifier.send_error_notification(str(exception), body, emails)
    #If too many retries happened
    if num_retries == retries: 
        send_max_retry_notifications(message_body)
    
def send_max_retry_notifications(message_body):
    package_id = message_body.get("package_id")
    msg_json = {
        "package_id": package_id,
        "transfer_status": "failure",
        "destination_path": message_body.get('destination_path'),
        "admin_metadata": {
            "original_queue": os.getenv("TRANSFER_PUBLISH_QUEUE_NAME"),
            "task_name": transfer_task,
            "retry_count": 0
        }
    }
    
    subject = "Maximum resubmitting retries reached for message with id {}.".format(message_body.get("package_id"))
    body = "Maximum resubmitting retries reached for message with id {}.\n\n" \
        "The message has been consumed and will not be resubmitted again.".format(message_body.get("package_id"))
    notifier.send_error_notification(subject, body)