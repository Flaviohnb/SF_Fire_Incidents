# San Francisco California - Fire Incidents

Project to analyze data on fires in the San Francisco area of California.
- Source Data: [Fire-Incidents](https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/about_data)

## Installation Instructions 

### 1 - Install docker

Docker is required for this project. Below are the instructions for installing Docker on Mac, Windows, and Linux. This project was developed on Windows.

- Windows: https://docs.docker.com/desktop/install/windows-install/
- Mac: https://docs.docker.com/desktop/install/mac-install/
- Linux: https://docs.docker.com/desktop/install/linux-install/

## Usage Guide

### 1 - Execute docker-compose

To run the 'Spark' and 'PostGreSQL' services on Docker. On our project root folder execute this command below with him, we can execute all services inside this file "docker-compose.yml".

- `$ docker-compose up -d`

I'll write more [docker commands](https://docs.docker.com/engine/reference/commandline/docker/) here:

- Stop and remove all container in your compose:
    - `$ docker-compose down`
- Show all containers with all status:
    - `$ docker ps -a` 
- Stop container:
    - `$ docker stop <CONTAINER ID>` 
- Delete container:
    - `$ docker rm -f <CONTAINER ID>` 
- Show all download images:
    - `$ docker images` 
- Delete image:
    - `$ docker rmi <IMAGE ID>`
- Entry in container:
    - `$ docker exec -it <CONTAINER NAME>/<CONTAINER ID> /bin/bash`
- Show all logs:
    - `$ docker logs -f <CONTAINER NAME>/<CONTAINER ID>`

### 2 - Create tables on PostgreSQL

In the docker-compose file, we mapped some [docker volumes](https://docs.docker.com/storage/volumes/). There are two folders, one inside the container and another on your computer, which reflect each other's files.
To create tables on PostgreSQL, follow the command below inside the /postgres-data folder:

- Execution permission:
    - `$ docker exec -it postgres-datawarehouse chmod +x ./volume/create_tables.sh` 
- Execution:
    - `$ docker exec -it postgres-datawarehouse ./volume/create_tables.sh`

Now you have these tables on PostgreSQL "fire_incidents" and "dim_calendar".

### 3 - Schedule Pipeline

Navigate to the "/pyspark-data/scripts_prod" folder and run the ETL scripts for datalake and data warehouse using the commands below. Consider setting up a [CRON](https://crontab.guru/) to automate these workflows.

- Execution permission:
    - `$ docker exec -it spark chmod +x ./home/jovyan/scripts_prod/workflow_fireincidents.sh` 
- Execution:
    - `$ docker exec -it spark ./home/jovyan/scripts_prod/workflow_fireincidents.sh`

- Execution permission:    
    - `$ docker exec -it spark chmod +x ./home/jovyan/scripts_prod/workflow_calendar.sh` 
- Execution:
    - `$ docker exec -it spark ./home/jovyan/scripts_prod/workflow_calendar.sh`

- Trigger the workflow, At 23:00 :
    - `$ docker exec -it spark 0 23 * * * ./home/jovyan/scripts_prod/workflow_fireincidents.sh`    

    