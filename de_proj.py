from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import datetime as dt
from datetime import timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import time
import logging as logger

default_args={
    'owner': 'de_proj_team',
    'start_date': dt.datetime(2022, 6, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=2),
}

def load_csv():
    logger.info('Loading the original data')
    df_orig = pd.read_csv('/mnt/projdata.csv')

    logger.info('Selecting the active loans')
    df_orig = df_orig[df_orig['Loan Status'] == "ACTIVE"]
    return df_orig.copy()

def db_connection():
    hostname = 'dev-postgres'
    port = '5432'
    username = 'postgres'
    password = 'password123'
    database = 'deproject'

    engine = create_engine(f'postgresql://{username}:{password}@{hostname}:{port}/{database}')
    return engine

def push_to_sql(ti):
    df_temp = ti.xcom_pull(task_ids='load_csv')
    engine = db_connection()
    logger.info('Pushing data to PostgreSQL')
    df_temp.to_sql('original_data',engine ,if_exists='replace', index=False)


def load_and_add_class(ti):
    logger.info('Loading Data and adding our class label')
    engine = db_connection()
    df = pd.read_sql('SELECT * FROM original_data', engine)
    df['our_class_label'] = np.where(df['No. of Days Delinquent'] >= 90, 'yes','no')
    
    return df.copy()

def feature_creation_selection(ti):
    logger.info('Dropping the not used feature for this training')
    df = ti.xcom_pull(task_ids='load_and_add_class')
    #select_feature = ['Customer Branch', 'Customer ID', 'Gender', 'Date of Birth', 'Age','Marital Status','Income','Business Line', 'Segment', 'Sector', 'Industry','Relationship Type', 'Join date','KYC Risk Level', 'Customer Status', 'Total Customer Deposits','Serial Number', 'Sales Branch', 'Main Product','Currency','Outstanding Balance','Outstanding Balance EQVL', 'Booking Date', 'Maturity Date','Approved Interest Rate ', 'Interest Rate', 'Interest Rate Band','Interest Rate Type','Remaining Tenor','Tenor Band', 'DBR Band','Payment Type','Payment Frequency', 'Approved Salary ', 'Salary Band','Delinquency Bucket','Delinquency Reason', 'Application Score', 'Finance to Value','Down Payment', 'Salesman', 'Loan Status']
    
    df = df.drop(['Customer Branch', 'Customer ID', 'Gender', 'Date of Birth', 'Age','Marital Status','Income','Business Line', 
    'Segment', 'Sector', 'Industry','Relationship Type', 'Join date','KYC Risk Level', 'Customer Status', 
    'Total Customer Deposits','Serial Number', 'Sales Branch', 'Main Product','Currency','Outstanding Balance',
    'Outstanding Balance EQVL', 'Booking Date', 'Maturity Date','Approved Interest Rate ', 'Interest Rate', 'Interest Rate Band',
    'Interest Rate Type','Remaining Tenor','Tenor Band', 'DBR Band','Payment Type','Payment Frequency', 'Approved Salary ', 'Salary Band',
    'Delinquency Bucket','Delinquency Reason', 'Application Score', 'Finance to Value','Down Payment', 'Salesman', 'Loan Status'], axis=1)
    #df['required_monthly_payment'] = df['Approved Amount EQVL'] / df['Tenor in Months']

    return df.copy()

def feature_rename(ti):
    logger.info('start with renaming the column names as part of preprocess')
    df = ti.xcom_pull(task_ids='feature_creation_selection')
    df = df.rename(columns={'Age Band':'age_band'})
    df = df.rename(columns={'Nationality':'nationality'})
    df = df.rename(columns={'Employment Status':'employment_status'})
    df = df.rename(columns={'Employer Category':'employer_category'})
    df = df.rename(columns={'Salary Transfer':'salary_transfer'})
    df = df.rename(columns={'Occupation':'occupation'})
    df = df.rename(columns={'Income Band':'income_band'})
    df = df.rename(columns={'Length of Relationship':'length_of_relationship'})
    df = df.rename(columns={'Product':'product'})
    df = df.rename(columns={'Approved Amount EQVL':'loan_value'})
    df = df.rename(columns={'Tenor in Months':'tenor_in_months'})
    df = df.rename(columns={'Monthly Payment':'monthly_payment'})
    df = df.rename(columns={'Has Delinquent Loans':'has_delinquent_loans'})
    df = df.rename(columns={'No. of Days Delinquent':'no_of_days_delinquent'})
    logger.info(df.info())

    return df.copy()

def tolower(df):
    for i in df.columns:
        try:
            df[i] = df[i].str.lower()
        except:
            pass

def removeSpace(df):
    for i in df.columns:
        try:
            df[i] = df[i].str.replace(' ','')
        except:
            pass

def data_preproces(ti):
    logger.info('Starting with data preprocessing')
    df = ti.xcom_pull(task_ids='feature_rename')
    tolower(df)
    removeSpace(df)
    df['age_band'] = df['age_band'].str.replace('age','')
    df['length_of_relationship'] = df['length_of_relationship'].str.replace('m','')
    df['income_band'] = df['income_band'].str.replace(';','')
    df = df.drop(['has_delinquent_loans','no_of_days_delinquent', 'monthly_payment'], axis=1)
    df['employer_category'] = df['employer_category'].replace("عسكري","militry")
    logger.info(df.info())

    return df.copy()

def fill_na(ti):
    df = ti.xcom_pull(task_ids='data_preproces')
    logger.info('Filling NaN in employment_status, employer_category and occupation as na and using it as a category')
    df['employment_status'] = df['employment_status'].fillna(value = 'na')
    df['employer_category'] = df['employer_category'].fillna(value = 'na')
    df['occupation'] = df['occupation'].fillna(value = 'na')
    df = df.dropna()
    logger.info(df.info())

    return df.copy()

def custom_label_encoder(df):
    logger.info('Doing a custom encoding for features age_band, income_band, length_of_relationship and our_class_label')
    age_index = { '<18': 0, '18-25': 1, '25-30': 2, '30-40': 3, '40-50': 4, '50-65': 5, '65+': 6}
    df['age_band'] = df['age_band'].map(age_index)
    logger.info(df['age_band'].value_counts())
    
    income_index = {'<250': 0,  '250-500': 1, '500-1000': 2, '1000-2000': 3,  '2000-5000': 4, '5000-10000': 5, '10000-15000': 6, '20000+': 7}
    df['income_band'] = df['income_band'].map(income_index)
    logger.info(df['income_band'].value_counts())
    
    relationship_in_month_index = {'<6': 0, '6-12': 1, '12-24': 2, '24-60': 3, '60-120': 4, '120+': 5}
    df['length_of_relationship'] = df['length_of_relationship'].map(relationship_in_month_index)
    logger.info(df['length_of_relationship'].value_counts())

    class_index = {'no':0, 'yes':1}
    df['our_class_label'] = df['our_class_label'].map(class_index)
    logger.info(df['our_class_label'].value_counts())

    salary_index = {'no':0, 'yes':1}
    df['salary_transfer'] = df['salary_transfer'].map(salary_index)
    logger.info(df['salary_transfer'].value_counts())

    return df.copy()

def encoding(ti):
    df_loaded = ti.xcom_pull(task_ids='fill_na')
    logger.info('Will do an encoding for the categorical features below as OneHot Encoding')
    df = custom_label_encoder(df_loaded)
    logger.info('Done with custom encoding, below how the data will look')
    logger.info(df.head(3))
    categorical_feature = ['nationality', 'employment_status', 'employer_category', 'occupation', 'product']
    from sklearn.preprocessing import OneHotEncoder as OHE
    ohe=OHE(handle_unknown="ignore")
    df_encode = df[categorical_feature]
    # Dropping those feature as they will be regoined after encoding
    df = df.drop(categorical_feature, axis=1)
    logger.info('Starting with the OneHot encoding')
    logger.info(df_encode.columns)
    df_done = pd.DataFrame(ohe.fit_transform(df_encode).toarray())
    # Reset index to join later
    df_done.reset_index(drop=True, inplace=True)
    #logger.info(df_done.info())
    df_done.columns = ohe.get_feature_names(categorical_feature)
    #logger.info(df_done.head())
    df_final = df.join(df_done)
    df_final = df_final.dropna()
    logger.info('We are done of the encoding, here is a look on the data')
    logger.info(df_final.head(20))

    return df_final

def scaling(ti):
    df = ti.xcom_pull(task_ids='encoding')
    numerical_feature = ['loan_value', 'tenor_in_months']
    from sklearn.preprocessing import MinMaxScaler
    min_max_scaler = MinMaxScaler()
    logger.info(f'Doing scaling for numerical values of {numerical_feature[0]} and {numerical_feature[1]}')
    logger.info(df.info())
    df_to_scale = df[numerical_feature]
    logger.info(df_to_scale.info())
    logger.info(df_to_scale.head(3))
    #df['loan_value'] = min_max_scaler.fit_transform(df['loan_value'])
    #df['tenor_in_months'] = min_max_scaler.fit_transform(df['tenor_in_months'])
    df_scaled = pd.DataFrame(min_max_scaler.fit_transform(df_to_scale), columns=numerical_feature)
    logger.info(df_scaled.head(10))
    df['loan_value'] = df_scaled['loan_value'].copy()
    df['tenor_in_months'] = df_scaled['tenor_in_months'].copy()
    logger.info(df.head(4))
    df = df.dropna()
    logger.info(df.info())

    return df

def balancing(ti):
    df = ti.xcom_pull(task_ids='scaling')
    from imblearn.over_sampling import SMOTE
    #from collection.Collection import Counter
    #counter_orignal = value_counts()
    #logger.info(f'Original Dataset shape: {counter_orignal}')
    logger.info('Starting rebalancing the data')
    smote = SMOTE()
    x = df.drop('our_class_label',axis=1)
    y = df['our_class_label']
    logger.info(y.value_counts())
    x_smote, y_smote = smote.fit_resample(x, y)
    logger.info(y_smote.value_counts())
    #counter_new = Counter(y_smote)
    #logger.info(f'New Dataset shape: {counter_new}')
    df = x_smote.copy()
    df['our_class_label'] = y_smote.copy()

    return df.copy()

def push_readyDF_to_sql(ti):
    df = ti.xcom_pull(task_ids='balancing')
    engine = db_connection()
    logger.info('Pushing ready data to PostgreSQL')    
    df.to_sql('ready_data', engine, if_exists='replace', index=False)

def write_to_csv(ti):
    df = ti.xcom_pull(task_ids='fill_na')
    #df.to_csv('/mnt/scaled_df.csv', index=False)
    df.to_csv('/mnt/before_encoding_df.csv', index=False)

def write_to_csv2(ti):
    df = ti.xcom_pull(task_ids='scaling')
    df.to_csv('/mnt/scaled_df.csv', index=False)
    #df.to_csv('/mnt/before_encoding_df.csv', index=False)

with DAG(
    'DE_Project_DAG',
    default_args=default_args,
    schedule_interval=timedelta(hours=2),
    catchup=False
) as dag:
    installing_modules = BashOperator(task_id='installing_modules', bash_command='pip install imbalanced-learn==0.8.1 scikit-learn matplot seaborn collection')
    load_csv = PythonOperator(task_id='load_csv', python_callable=load_csv)
    push_to_sql = PythonOperator(task_id='push_to_sql', python_callable=push_to_sql)
    load_and_add_class = PythonOperator(task_id='load_and_add_class', python_callable=load_and_add_class)
    feature_creation_selection = PythonOperator(task_id='feature_creation_selection', python_callable=feature_creation_selection)
    feature_rename = PythonOperator(task_id='feature_rename', python_callable=feature_rename)
    data_preproces = PythonOperator(task_id='data_preproces', python_callable=data_preproces)
    fill_na = PythonOperator(task_id='fill_na', python_callable=fill_na)
    encoding = PythonOperator(task_id='encoding', python_callable=encoding)
    scaling = PythonOperator(task_id='scaling', python_callable=scaling)
    balancing = PythonOperator(task_id='balancing', python_callable=balancing)
    push_readyDF_to_sql = PythonOperator(task_id='push_readyDF_to_sql', python_callable=push_readyDF_to_sql)
    #write_to_csv = PythonOperator(task_id='write_to_csv', python_callable=write_to_csv)
    #write_to_csv2 = PythonOperator(task_id='write_to_csv2', python_callable=write_to_csv2)

    installing_modules >> load_csv >> push_to_sql >> load_and_add_class >> feature_creation_selection >> feature_rename >> data_preproces >> fill_na >> encoding >> scaling >> balancing >> push_readyDF_to_sql
    #installing_modules >> load_csv >> push_to_sql >> load_and_add_class >> feature_creation_selection >> feature_rename >> data_preproces >> fill_na >> write_to_csv >> encoding >> scaling >> write_to_csv2