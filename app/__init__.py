from flask import Flask
from healthcheck import HealthCheck, EnvironmentDump
import mqresources.mqutils as mqutils
import os


'''This class is currently entirely for the purpose of providing
a healthcheck '''

# App factory
def create_app():
    app = Flask(__name__)

    health = HealthCheck()
    
    includeenvdump = True
    if(os.getenv('ENV', 'production') == 'production'): 
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
    return app
