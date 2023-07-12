from celery import Celery
import os
import traceback
import transfer_service.transfer_ready_validation as transfer_ready_validation
import transfer_service.transfer_service as transfer_service
import notifier.notifier as notifier

app = Celery()
app.config_from_object('celeryconfig')

logger = logging.getLogger('transfer-service')

@app.task(serializer='json', name='transfer_service.tasks.transfer_data')
def transfer_data(message_body):
    logger.debug("Message Body: {}".format(message_body))
    # Do not do the validation and transfer if dry_run is set
    if "dry_run" in message_body:
        return "hello"

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
        send_error_notifications(msg, message_body, e, exception_msg, failureEmail)
    except TransferException as e:
        msg = str(e)
        exception_msg = traceback.format_exc()
        failureEmail = message_body["admin_metadata"]["failureEmail"]
        exception_msg = traceback.format_exc()
        send_error_notifications(msg, message_body, e, exception_msg, failureEmail)
    except Exception as e:
        msg = str(e)
        exception_msg = traceback.format_exc()
        body = msg + "\n" + exception_msg
        send_error_notifications(msg, message_body, e, exception_msg)

def send_error_notifications(message_start, message_body, exception, exception_msg, emails=None):
    package_id = message_body.get("package_id")
    msg_json = {
        "package_id": package_id,
        "transfer_status": "failure",
        "destination_path": message_body.get('destination_path'),
        "admin_metadata": {
            "original_queue": os.getenv("TRANSFER_PUBLISH_QUEUE_NAME"),
            "retry_count": 0
        }
    }
    app.send_task("tasks.tasks.do_task", args=[msg_json], kwargs={},
            queue=os.getenv("TRANSFER_PUBLISH_QUEUE_NAME"))
    body = message_start + "\n" + exception_msg
    notifier.send_error_notification(str(exception), body, emails)