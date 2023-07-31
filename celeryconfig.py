import os

broker_url = os.getenv('BROKER_URL')
broker_connection_retry_on_startup=True
task_serializer = 'json'
accept_content = ['application/json']
result_serializer = 'json'
timezone = 'US/Eastern'
enable_utc = True
worker_enable_remote_control = False

task_routes = {
    'transfer_service.tasks.transfer_data':
        {'queue': os.getenv("TRANSFER_CONSUME_QUEUE_NAME")}
}
