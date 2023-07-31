# dais-transfer-service
A service to transfer data from S3 to the dropbox

## Local setup
    
1. Make a copy of the env-template.txt to .env and modify the user and password variables.

2. Start the container
    
```
docker-compose -f docker-compose-local.yml up -d --build --force-recreate
```

Local Healthcheck: https://localhost:10584/healthcheck

## Testing
Note, testing uses its own queues so they will not interfere with the queues used by the actual program.

1. Start the container up as described in the <b>Local Setup</b> instructions.

2. Exec into the container:

```
docker exec -it dais-transfer-service bash
```

3. Run the tests

```
pytest
```

## Invoking the task manually

### invoke-task.py (add message to the transfer service queue)

- Clone this repo from github 

- Create the .env from the env-template.txt and replace with proper values (use LPE Shared-DAIS for passwords)

- Start up docker  

`docker-compose -f docker-compose-local.yml up --build -d --force-recreate`

- Exec into the docker container

`docker exec -it dais-transfer-service bash`

- Run invoke task python script

`python3 scripts/invoke-task.py`

- Bring up [DEV DAIS Rabbit UI](https://b-e9f45d5f-039d-4226-b5df-1a776c736346.mq.us-east-1.amazonaws.com/)  - credentials in LPE Shared-DAIS

- Look for the queue with `-dryrun` (eg `transfer-status-dryrun`) appended to its name and verify a message made it

