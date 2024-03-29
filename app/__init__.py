import logging
import os
from logging.handlers import TimedRotatingFileHandler

from flask import Flask
from healthcheck import HealthCheck, EnvironmentDump

LOG_FILE_BACKUP_COUNT = 1
LOG_ROTATION = "midnight"


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


    # add your own data to the environment dump
    def application_data():
        return {"maintainer": "Harvard Library Technology Services",
                "git_repo": "https://github.com/harvard-lts/hdc3a-transfer-service",
                "version": os.getenv('VERSION', "Not Supplied")}

    health.add_section("application", application_data)

    envdump.add_section("application", application_data)

    # Add a flask route to expose information
    app.add_url_rule("/healthcheck", "healthcheck", view_func=health.run)
    app.add_url_rule("/environment", "environment", view_func=envdump.run)

    
    return app


def configure_logger():
    log_level = os.getenv("LOGLEVEL", "WARNING")
    log_file_path = os.getenv("LOGFILE_PATH", "/home/appuser/epadd-curator-app/logs/transfer_service.log")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    file_handler = TimedRotatingFileHandler(
        filename=log_file_path,
        when=LOG_ROTATION,
        backupCount=LOG_FILE_BACKUP_COUNT
    )
    logger = logging.getLogger('transfer-service')
        
    logger.addHandler(file_handler)
    file_handler.setFormatter(formatter)
    logger.setLevel(log_level)
