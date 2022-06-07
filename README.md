# Data Engineering Project

#### This reposiroty contains the implementation of the project for data engineering course

## Description of current fils:
- dags: the folder that is mounted for airflow containers and contain the DAG implementation
- Data Engineer Presetation.pptx: The presentation for the project
- de_proj.py: the DAG implementation for the project
- docker-compose.yaml: the docker-compose file to build the infrastructure used for this project
- esdata1: The volume that is mounted for elastic search containers, it is kept as it has the dashboards already implemented and saved inside of it
- input: a volume that is mounted under /mnt for airflow workers so that the DAG can use this path to read the data
- KibanaDashboard.ndjson: the exported file for dashboards that are implemented on Kibana
- pgadmin: the volume that is mounted inside of pgadmin container, and it is kept as it has the connection to DB node already saved in it.
- prepareenv.sh: The script that used to build the infrastructure and install all of the needed pyton models in airflow worker
- projdata.csv: a csv file containing that data used for this project.

---

## Tools used in this project:
- (Airflow)[https://airflow.apache.org/]
- Elasticsearch
- Kibana
- 