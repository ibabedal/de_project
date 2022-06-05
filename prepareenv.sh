#!/bin/bash

## Prepare the pgadmin, notebook and postgres
mkdir ./notebooks ./pgvol ./pgadmin ./esdata1
chmod 777 ./pgadmin
cp ../projdata.csv ../filtered_data.csv notebooks/

## Prepare airflow setup
mkdir ./dags ./logs ./plugins ./af_pgvol ./input
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
echo "ELASTIC_VERSION=7.8.1" >> .env
cp de_proj.py dags/
cp projdata.csv input/

docker-compose up -d airflow-init
docker-compose up -d