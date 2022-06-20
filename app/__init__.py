import logging
import os
from logging.handlers import RotatingFileHandler

import mqresources.mqutils as mqutils
from flask import Flask
from healthcheck import HealthCheck, EnvironmentDump
from mqresources.transfer_ready_queue_listener import TransferReadyQueueListener

# TODO
'''This class is currently entirely for the purpose of providing
a healthcheck and initializing queue listener'''

LOG_FILE_DEFAULT_PATH = "hdc3a_transfer_service"
LOG_FILE_DEFAULT_LEVEL = logging.WARNING
LOG_FILE_MAX_SIZE_BYTES = 2 * 1024 * 1024
LOG_FILE_BACKUP_COUNT = 1


# App factory
def create_app():
    configure_logger()

    app = Flask(__name__)

    health = HealthCheck()

    includeenvdump = True
    if (os.getenv('ENV', 'production') == 'production'):
        includeenvdump = False

    envdump = EnvironmentDump(include_python=includeenvdump,
                              include_os=includeenvdump,
                              include_process=includeenvdump)

    # add a check for the process mq connection
    def checktransfermqconnection():
        connection_params = mqutils.get_transfer_mq_connection()
        if connection_params.conn is None:
            return False, "transfer mq connection failed"
        connection_params.conn.disconnect()
        return True, "transfer mq connection ok"

    # add your own data to the environment dump
    def application_data():
        return {"maintainer": "Harvard Library Technology Services",
                "git_repo": "https://github.com/harvard-lts/hdc3a-transfer-service",
                "version": os.getenv('VERSION', "Not Supplied")}

    health.add_check(checktransfermqconnection)
    health.add_section("application", application_data)

    envdump.add_section("application", application_data)

    # Add a flask route to expose information
    app.add_url_rule("/healthcheck", "healthcheck", view_func=health.run)
    app.add_url_rule("/environment", "environment", view_func=envdump.run)

    # Initializing queue listener
    logging.debug("--------- CREATING THE LISTENER ---------")
    TransferReadyQueueListener()

    return app


def configure_logger():
    log_file_path = os.getenv('LOGFILE_PATH', LOG_FILE_DEFAULT_PATH)
    logger = logging.getLogger()

    file_handler = RotatingFileHandler(
        filename=log_file_path,
        maxBytes=LOG_FILE_MAX_SIZE_BYTES,
        backupCount=LOG_FILE_BACKUP_COUNT
    )
    logger.addHandler(file_handler)

    log_level = os.getenv('LOGLEVEL', LOG_FILE_DEFAULT_LEVEL)
    logger.setLevel(log_level)
