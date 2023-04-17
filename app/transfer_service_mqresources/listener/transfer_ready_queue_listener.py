import os

import transfer_service.transfer_ready_validation as transfer_ready_validation
import transfer_service.transfer_service as transfer_service
from transfer_service_mqresources import mqutils
from transfer_service_mqresources.listener.mq_connection_params import MqConnectionParams
from transfer_service_mqresources.listener.stomp_listener_base import StompListenerBase


class TransferReadyQueueListener(StompListenerBase):

    def _get_queue_name(self) -> str:
        return os.getenv('TRANSFER_QUEUE_CONSUME_NAME')

    def _get_mq_connection_params(self) -> MqConnectionParams:
        return MqConnectionParams(
            mq_host=os.getenv('TRANSFER_MQ_HOST'),
            mq_port=os.getenv('TRANSFER_MQ_PORT'),
            mq_user=os.getenv('TRANSFER_MQ_USER'),
            mq_password=os.getenv('TRANSFER_MQ_PASSWORD')
        )

    def _handle_received_message(self, message_body: dict, message_id: str, message_subscription: str) -> None:
        self._logger.debug("************************ TRANSFER READY LISTENER - ON_MESSAGE ************************")
        self._logger.debug(
            "Received message from Transfer Queue. Message body: {}. Message id: {}".format(
                str(message_body),
                message_id
            )
        )

        self._acknowledge_message(message_id, message_subscription)

        # Do not do the validation and transfer if dry_run is set
        if "dry_run" in message_body:
            return

        try:
            # Validate json
            transfer_ready_validation.validate_json_schema(message_body)

            # Transfer data
            self._logger.debug(
                'TRANSFERRING DATA {} to {}'.format(message_body['s3_path'], message_body['destination_path'])
            )
            transfer_service.transfer_data(message_body)
        except ValidationException as e:
            transfer_status = mqutils.TransferStatus(
                message_body.get("package_id"),
                "failure",
                message_body.get('destination_path')
            )
            mqutils.notify_transfer_status_message(transfer_status)
            msg = "Validation failed so transfer was not completed"
            exception_msg = traceback.format_exc()
            body = msg + "\n" + exception_msg
            notifier.send_error_notification(str(e), body)
        except TransferException as e:
            transfer_status = mqutils.TransferStatus(
                message_body.get("package_id"),
                "failure",
                message_body.get('destination_path')
            )
            mqutils.notify_transfer_status_message(transfer_status)
            msg = str(e)
            exception_msg = traceback.format_exc()
            body = msg + "\n" + exception_msg
            notifier.send_error_notification(str(e), body)
        except Exception as e:
            msg = str(e)
            exception_msg = traceback.format_exc()
            body = msg + "\n" + exception_msg
            notifier.send_error_notification(str(e), body)

