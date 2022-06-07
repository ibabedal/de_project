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
- [Knime](https://www.knime.com/)
- [Docker](https://www.docker.com/)
- [Airflow](https://airflow.apache.org/)
- [Elasticsearch](https://www.elastic.co/elasticsearch/)
- [Kibana](https://www.elastic.co/kibana)
- [PostgreSQL](https://www.postgresql.org/)

---

## how to run:
- Please clone the repository into your computer
- Please run the script '**prepareenv.sh**' and it will prepare everything to you

---

## workflow description:

- load_csv: to read the csv file from /mnt
- push_to_sql: to push the data as is without any modification to the postgres database
- load_and_feature_engineering: to load the data from the SQL, and perform feature enigneering on it which includes:
    - Add the class label based on business requirements that the customer is defaulted if number of days of delinquent is more than 90
    - Drop some features that are not related to the analysis based business relation (like customer ID) or features that have a high correlation based on a correlation matrix that were preprared before hand
    - Doing a rename for the features to be more easy to query and remote the white spaces from the names, and the % from inside of some features entries as they will later affect the push to sql after encoding
- handing_missing_data: impute the missing values for categorical features with mode or 'Others' as a new category since it is expected that these values will be filled empty, update the numerical values with mean value.
- push_to_elastic: pushing the ready data to elastic to create insights.
- encoding: to proceed with performing a OneHotEncoding for the categorical variable, and a manual label encoding for some of the binary categorical variables
- scaling: perform scaling on the numerical values
- push_readyDF_to_sql: to send the prepared data frame for SQL so in case a further DAGs want to train different models they can load the data from this ready table in SQL
- split-and_balance_training: since our problem is a classification for imbalanced data, on this task we split the data between training and testing and perform SMOTE to balance the data.
- 4 different task to train 4 different models which are: decision tree, random forest, bagging and xgboost
- 4 scoring tasks that is used to get the model accuracy, recall, precision and f1-score from each model
- push_score_to_es: to push those score records to elastic search so a the dashboards can be drawn for those scores, and the dashboards can used to monitor the scoring on the models

---

## Additional information:

- The data is passed between the tasks in the DAG using the [XCOM](https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html)
- Other internal fuctions are used inside of the DAG to avoid writing a code, like a function to create the engine to connect to DB, the details about them are in the DAG itself