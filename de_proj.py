from platform import python_compiler
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
    'start_date': dt.datetime(2022, 6, 5),
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
    df.rename(columns= {'No. of Days Delinquent': 'NoDaysDelinquent'}, inplace=True)
    df['Label'] = np.where(df['NoDaysDelinquent'] >= 90, 1, 0)
    
    return df.copy()

def feature_selection(ti):
    logger.info('Dropping the not used feature for this training')
    df = ti.xcom_pull(task_ids='load_and_add_class')
    #select_feature = ['Customer Branch', 'Customer ID', 'Gender', 'Date of Birth', 'Age','Marital Status','Income','Business Line', 'Segment', 'Sector', 'Industry','Relationship Type', 'Join date','KYC Risk Level', 'Customer Status', 'Total Customer Deposits','Serial Number', 'Sales Branch', 'Main Product','Currency','Outstanding Balance','Outstanding Balance EQVL', 'Booking Date', 'Maturity Date','Approved Interest Rate ', 'Interest Rate', 'Interest Rate Band','Interest Rate Type','Remaining Tenor','Tenor Band', 'DBR Band','Payment Type','Payment Frequency', 'Approved Salary ', 'Salary Band','Delinquency Bucket','Delinquency Reason', 'Application Score', 'Finance to Value','Down Payment', 'Salesman', 'Loan Status']
    
    df = df.drop(['Customer ID', 'Customer Branch', 'Age','Income','Serial Number',
                                'Occupation','Segment','Sector','Outstanding Balance EQVL',
              'Product','Currency','Booking Date', 'Join date', 'Sales Branch', 
              'Maturity Date','Approved Interest Rate ','Interest Rate','Tenor Band',
             'Payment Type','Approved Salary ','Has Delinquent Loans', 'NoDaysDelinquent',
             'Delinquency Reason','Application Score','Finance to Value','Down Payment','Salesman','Loan Status',
                               'Date of Birth'], axis = 1)
    #df['required_monthly_payment'] = df['Approved Amount EQVL'] / df['Tenor in Months']

    return df.copy()

def feature_rename(ti):
    logger.info('start with renaming the column names as part of preprocess')
    df = ti.xcom_pull(task_ids='feature_selection')
    df.rename(columns= {
                        'Gender': 'Gender',
                        'Age Band': 'AgeBand', 
                        'Nationality': 'Nationality',
                        'Marital Status': 'MaritalStatus',
                        'Employment Status': 'EmploymentStatus',
                        'Employer Category': 'EmployerCategory',
                        'Salary Transfer': 'SalaryTransfer',
                        'Income Band': 'IncomeBand',
                        'Total Customer Deposits': 'TotalCustomerDeposits',
                        'Business Line': 'BusinessLine',
                        'Industry': 'Industry',
                        'Relationship Type': 'RelationshipType',
                        'Length of Relationship': 'LengthOfRelationship',
                        'KYC Risk Level': 'KYCRiskLevel',
                        'Customer Status': 'CustomerStatus',
                        'Total Customer Deposit':'TotalCustomerDeposit',
                        'Main Product' : 'MainProduct',
                        'Approved Amount EQVL' : 'ApprovedAmountEQVL',
                        'Outstanding Balance' : 'OutstandingBalance',
                        'Interest Rate Band': 'InterestRateBandPercent',
                        'Interest Rate Type' :'InterestRateType',
                        'Tenor in Months' : 'TenorInMonths',
                        'Remaining Tenor' : 'RemainingTenor',
                        'DBR Band' : 'DBRBandPercent',
                        'Monthly Payment': 'MonthlyPayment',
                        'Payment Frequency': 'PaymentFrequency',
                        'Salary Band': 'SalaryBand',
                        'Delinquency Bucket': 'DelinquencyBucket'
                        
                    }, inplace = True)
    logger.info(df.info())

    return df.copy()


def data_preproces(ti):
    logger.info('Starting with data preprocessing')
    df = ti.xcom_pull(task_ids='feature_rename')
    df = df.drop(['MonthlyPayment', 'CustomerStatus', 'RelationshipType'], axis=1)
    df.drop(['SalaryBand','BusinessLine', 'OutstandingBalance','InterestRateType','RemainingTenor','DelinquencyBucket'], axis=1, inplace=True)
    df.drop(df[df['Industry'].isnull()].index, inplace = True)
    logger.info(df.shape)
    df.drop(df[df['KYCRiskLevel'] == 0].index, inplace = True)
    logger.info(df.shape)
    df.drop(df[df['Gender'].isnull()].index, inplace = True)
    logger.info(df.shape)
    df['InterestRateBandPercent'] = df['InterestRateBandPercent'].str.replace('%','')
    df['DBRBandPercent'] = df['DBRBandPercent'].str.replace('%','')
    logger.info(df.info())

    return df.copy()

def handling_mising_data(ti):
    df = ti.xcom_pull(task_ids='data_preproces')
    logger.info('Filling the missing values, and drop rows of missing values with less than 0.001')
    logger.info('Printing the shape of the DF to confirm that there is no changes in these values')
    df.drop(df[df['MaritalStatus'].isnull()].index, inplace = True)
    logger.info(df.shape)

    logger.info('Now we will impute the missing values for the following features, with mode for categorical, and mean for numerical')
    df.loc[df.EmployerCategory.isna(), "EmployerCategory"] = "Others"
    df.loc[df.EmployerCategory.str.contains("عسكري", case =False), "EmployerCategory"] = "Military"
    df['PaymentFrequency'].fillna('M', inplace=True)
    df['EmploymentStatus'].fillna('FULL TIME EMPLOYED', inplace=True)
    df['TotalCustomerDeposits'].fillna(value=df['TotalCustomerDeposits'].mean(), inplace=True)
    logger.info(df.info())
    logger.info(df.head())
    return df.copy()

def custom_label_encoder(df):
    logger.info('Doing a custom encoding for features age_band, income_band, length_of_relationship and our_class_label')
    gender_index={'FEMALE':0, 'MALE':1}
    df['Gender'] = df['Gender'].map(gender_index)
    logger.info(df['Gender'].value_counts())

    salary_transfer_index = {'No':0,'Yes':1}
    df['SalaryTransfer'] = df['SalaryTransfer'].map(salary_transfer_index)
    logger.info(df['SalaryTransfer'].value_counts())

    return df.copy()

def encoding(ti):
    df_loaded = ti.xcom_pull(task_ids='handling_mising_data')
    logger.info('Will do an encoding for the categorical features below as OneHot Encoding')
    df = custom_label_encoder(df_loaded)
    logger.info('Done with custom encoding, below how the data will look')
    logger.info(df.head(3))
    categorical_feature = ['AgeBand','Nationality','MaritalStatus','EmploymentStatus','EmployerCategory','IncomeBand','Industry',
 'LengthOfRelationship','KYCRiskLevel','MainProduct','InterestRateBandPercent','DBRBandPercent','PaymentFrequency']
    from sklearn.preprocessing import OneHotEncoder as OHE
    ohe=OHE(handle_unknown="ignore")
    df_encode = df[categorical_feature]
    # Dropping those feature as they will be regoined after encoding
    df = df.drop(categorical_feature, axis=1)
    logger.info('Starting with the OneHot encoding')
    logger.info(df_encode.columns)
    df_done = pd.DataFrame(ohe.fit_transform(df_encode).toarray())
#    # Reset index to join later
#    df_done.reset_index(drop=True, inplace=True)
#    #logger.info(df_done.info())
#    df_done.columns = ohe.get_feature_names(categorical_feature)
#    #logger.info(df_done.head())
#    df_final = df.join(df_done)
#    df_final = df_final.dropna()
    df_done.reset_index(drop=True, inplace=True)
    df_done.columns = ohe.get_feature_names(categorical_feature)
    df.reset_index(drop=True, inplace=True)
    df_final = df.join(df_done)
    logger.info('We are done of the encoding, here is a look on the data')
    logger.info(df_final.head(20))

    return df_final

def scaling(ti):
    df = ti.xcom_pull(task_ids='encoding')
    numerical_feature = ['TotalCustomerDeposits','ApprovedAmountEQVL','TenorInMonths']
    from sklearn.preprocessing import MinMaxScaler
    min_max_scaler = MinMaxScaler()
    logger.info(f'Doing scaling for numerical values of {numerical_feature[0]}, {numerical_feature[1]} and {numerical_feature[2]}')
    logger.info(df.info())
    #df_to_scale = df[numerical_feature]
    #logger.info(df_to_scale.info())
    #logger.info(df_to_scale.head(3))
    #df['loan_value'] = min_max_scaler.fit_transform(df['loan_value'])
    #df['tenor_in_months'] = min_max_scaler.fit_transform(df['tenor_in_months'])
    df_scaled = pd.DataFrame(min_max_scaler.fit_transform(df[numerical_feature]), columns=numerical_feature)
    logger.info(df_scaled.head(10))
    df[numerical_feature[0]] = df_scaled[numerical_feature[0]].copy()
    df[numerical_feature[1]] = df_scaled[numerical_feature[1]].copy()
    df[numerical_feature[2]] = df_scaled[numerical_feature[2]].copy()
    logger.info(df.head(4))
    #df = df.dropna()
    logger.info(df.info())

    return df

def push_readyDF_to_sql(ti):
    df = ti.xcom_pull(task_ids='scaling')
    engine = db_connection()
    logger.info('Pushing ready scaled data to PostgreSQL')    
    df.to_sql('ready_data', engine, if_exists='replace', index=False)

def cf_mat_plot(y_test,y_pred):
    from sklearn.metrics import confusion_matrix
    from sklearn.metrics import accuracy_score,roc_auc_score,f1_score
    from sklearn import metrics
    #import seaborn as sns
    #import matplotlib.pyplot as plt
    cf_matrix = confusion_matrix(y_test, y_pred, labels=[1,0])
    print(cf_matrix)
    #ax = sns.heatmap(cf_matrix, annot=True, cmap='Blues')
    #ax.set_title('Seaborn Confusion Matrix with labels\n\n');
    #ax.set_xlabel('\nPredicted Values')
    #ax.set_ylabel('Actual Values ');
    ## Ticket labels - List must be in alphabetical order
    #ax.xaxis.set_ticklabels(['1','0'])
    #ax.yaxis.set_ticklabels(['1','0'])
    ## Display the visualization of the Confusion Matrix.
    #plt.show()
    print('TP:', cf_matrix[0][0])
    print('TN:', cf_matrix[1][1])
    print('FP:', cf_matrix[1][0])
    print('FN:', cf_matrix[0][1])
    print('Accuracy score:',accuracy_score(y_test, y_pred))
    print('Precision score', metrics.precision_score(y_test, y_pred))
    print('Recall score', metrics.recall_score(y_test, y_pred))
    print('F1 score:',f1_score(y_test, y_pred))

def split_and_balance_training(ti):
    logger.info('We will need to load the scaled data from SQL in order to proceed with training the models')
    engine = db_connection()
    df = pd.read_sql('SELECT * FROM ready_data', engine)
    from imblearn.over_sampling import SMOTE
    from sklearn.model_selection import train_test_split
    #from collection.Collection import Counter
    #counter_orignal = value_counts()
    #logger.info(f'Original Dataset shape: {counter_orignal}')
    logger.info('We will first split the data into training and testing')
    smote = SMOTE()
    x = df.drop('Label',axis=1)
    y = df['Label']
    x_train, x_test, y_train, y_test = train_test_split(x.values, y.values, test_size = 0.3, random_state = 42, stratify=y)


    x_smote, y_smote = smote.fit_resample(x_train, y_train)

    return x_smote, y_smote, x_test, y_test

def dt_train_test(ti):
    x_smote, y_smote, x_test, y_test = ti.xcom_pull(task_ids='split_and_balance_training')
    from sklearn import tree
    DT = tree.DecisionTreeClassifier(criterion='gini')
    DT = DT.fit(x_smote, y_smote)

    y_pred = DT.predict(x_test)
    return y_test, y_pred

def dt_score(ti):
    y_test, y_pred = ti.xcom_pull(task_ids='dt_train_test')
    cf_mat_plot(y_test, y_pred)

def rf_train_test(ti):
    x_smote, y_smote, x_test, y_test = ti.xcom_pull(task_ids='split_and_balance_training')
    from sklearn.ensemble import RandomForestClassifier
    rfc = RandomForestClassifier()
    # fit the predictor and target
    rfc.fit(x_smote, y_smote)
    # predict
    rfc_predict = rfc.predict(x_test)
    return y_test, rfc_predict

def rf_score(ti):
    y_test, y_pred = ti.xcom_pull(task_ids='rf_train_test')
    cf_mat_plot(y_test, y_pred)

def bagging_train_test(ti):
    x_smote, y_smote, x_test, y_test = ti.xcom_pull(task_ids='split_and_balance_training')
    from sklearn.ensemble import BaggingClassifier
    model = BaggingClassifier()
    model.fit(x_smote, y_smote)
    y_pred = model.predict(x_test)

    return y_test, y_pred

def bagging_score(ti):
    y_test, y_pred = ti.xcom_pull(task_ids='bagging_train_test')
    cf_mat_plot(y_test, y_pred)

def xgboost_train_test(ti):
    x_smote, y_smote, x_test, y_test = ti.xcom_pull(task_ids='split_and_balance_training')
    from xgboost import XGBClassifier
    model = XGBClassifier()
    model.fit(x_smote, y_smote)
    #print(model)
    # make predictions for test data
    y_pred = model.predict(x_test)
    #print(y_pred >= 0.5)
    predictions = [round(value) for value in y_pred]
    #print(predictions)
    return y_test, predictions

def xgboost_score(ti):
    y_test, y_pred = ti.xcom_pull(task_ids='xgboost_train_test')
    cf_mat_plot(y_test, y_pred)


def write_to_csv(ti):
    df = ti.xcom_pull(task_ids='handling_mising_data')
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
    installing_modules = BashOperator(task_id='installing_modules', bash_command='pip install imbalanced-learn==0.8.1 scikit-learn matplot seaborn collection xgboost')
    #load_csv = PythonOperator(task_id='load_csv', python_callable=load_csv)
    #push_to_sql = PythonOperator(task_id='push_to_sql', python_callable=push_to_sql)
    #load_and_add_class = PythonOperator(task_id='load_and_add_class', python_callable=load_and_add_class)
    #feature_selection = PythonOperator(task_id='feature_selection', python_callable=feature_selection)
    #feature_rename = PythonOperator(task_id='feature_rename', python_callable=feature_rename)
    #data_preproces = PythonOperator(task_id='data_preproces', python_callable=data_preproces)
    #handling_mising_data = PythonOperator(task_id='handling_mising_data', python_callable=handling_mising_data)
    #encoding = PythonOperator(task_id='encoding', python_callable=encoding)
    #scaling = PythonOperator(task_id='scaling', python_callable=scaling)
    #push_readyDF_to_sql = PythonOperator(task_id='push_readyDF_to_sql', python_callable=push_readyDF_to_sql)
    #write_to_csv = PythonOperator(task_id='write_to_csv', python_callable=write_to_csv)
    #write_to_csv2 = PythonOperator(task_id='write_to_csv2', python_callable=write_to_csv2)
    split_and_balance_training = PythonOperator(task_id='split_and_balance_training', python_callable=split_and_balance_training)
    dt_train_test = PythonOperator(task_id='dt_train_test', python_callable=dt_train_test)
    dt_score = PythonOperator(task_id='dt_score', python_callable=dt_score)
    rf_train_test = PythonOperator(task_id='rf_train_test', python_callable=rf_train_test)
    rf_score = PythonOperator(task_id='rf_score', python_callable=rf_score)
    bagging_train_test = PythonOperator(task_id='bagging_train_test', python_callable=bagging_train_test)
    bagging_score = PythonOperator(task_id='bagging_score', python_callable=bagging_score)
    xgboost_train_test = PythonOperator(task_id='xgboost_train_test', python_callable=xgboost_train_test)
    xgboost_score = PythonOperator(task_id='xgboost_score', python_callable=xgboost_score)

    #installing_modules >> load_csv >> push_to_sql >> load_and_add_class >> feature_selection >> feature_rename >> data_preproces >> handling_mising_data >> encoding >> scaling >> push_readyDF_to_sql >> split_and_balance_training
    split_and_balance_training >> dt_train_test >> dt_score
    split_and_balance_training >> rf_train_test >> rf_score
    split_and_balance_training >> bagging_train_test >> bagging_score
    split_and_balance_training >> xgboost_train_test >> xgboost_score