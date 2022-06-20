import logging
import os

import transfer_service.transfer_ready_validation as transfer_ready_validation
import transfer_service.transfer_service as transfer_service
from mqresources import mqutils
from mqresources.listener.mq_connection_params import MqConnectionParams
from mqresources.listener.stomp_listener_base import StompListenerBase


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
        self._logger.info(
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
            logging.debug(
                'TRANSFERRING DATA {} to {}'.format(message_body['s3_path'], message_body['destination_path'])
            )
            transfer_service.transfer_data(message_body)
        except Exception:
            # TODO: NACKs for malformed messages
            transfer_status = mqutils.TransferStatus(
                message_body.get("package_id"),
                "failure",
                message_body.get('destination_path')
            )
            # mqutils.notify_transfer_status_message(transfer_status)
            # logging.exception("validation failed so transfer was not completed")
