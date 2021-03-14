import pandas as pd
from airflow.models import Variable
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2021, 3, 10, 00, 00, 00)
}


with DAG(
    dag_id='etl_dag',
    default_args=default_args,
    description='ETL DAG',
    schedule_interval= timedelta(minutes=20),
    tags=['etl'],
) as dag:
    dag.doc_md = __doc__

    outputEtlPath = Variable.get('outputEtlPath')
    logFileName = Variable.get('logFileName')

    def extract(**kwargs):
        pg_hook = MySqlHook(mysql_conn_id='dbsource')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        tables = Variable.get("sourceTableNames", deserialize_json=True)
        log_file = open(outputEtlPath+logFileName, "w")
        log_file.write('{0:30}{1:15}\n'.format('sourceTableNames','sourceTableCount'))

        for table in tables:
            query = f'SELECT * FROM {table}'
            cursor.execute(query)
            sources = cursor.fetchall()
            log_file.write('{0:30}{1:15}\n'.format(table,len(sources)))

            csv_table = pg_hook.get_pandas_df(query)
            csv_table.to_csv(outputEtlPath+f'{table}.csv', index = False)
            
    
        log_file.close()

    def transform(**kwargs):    
        mainTable = Variable.get("mainTable")
        finalCsvName = Variable.get("finalCsvName")
        joinTable = Variable.get("joinTable", deserialize_json=True)
        mainTable_csv = pd.read_csv(outputEtlPath+f'{mainTable}.csv')

        for join in joinTable:
            join_csv = pd.read_csv(outputEtlPath+f'{join}.csv')
            mainTable_csv = mainTable_csv.merge(join_csv, how='left',left_on=f'{join}_ID', right_on='ID', suffixes=('',f'_{join}'))
            mainTable_csv.drop(mainTable_csv.filter(regex=f'ID_{join}$').columns.tolist(),axis=1, inplace=True)

        mainTable_csv = mainTable_csv.to_csv(outputEtlPath + f'{finalCsvName}.csv',index = False)

    def load(**kwargs):
        alchemyEngine = create_engine(Variable.get('postgresUrl'))
        postgreSQLConnection= alchemyEngine.connect()
        targetTables = Variable.get('targetTableNames', deserialize_json=True)
        log_file = open(outputEtlPath + logFileName, "a")
        log_file.write('{0:30}{1:15}\n'.format('targetTableNames','targetTableCount'))

        for table in targetTables:
            finaly_csv = pd.read_csv(outputEtlPath+f'{table}.csv')
            columnsTypeFormating = finaly_csv.filter(regex=f'(_)?DATE$').columns.tolist()
            for columnTypeFormating in columnsTypeFormating:
                finaly_csv[columnTypeFormating] =  pd.to_datetime(finaly_csv[columnTypeFormating], format='%Y/%m/%d')
            
            finaly_csv.to_sql(f'{table}', con=postgreSQLConnection,if_exists='replace', index=False)
            log_file.write('{0:30}{1:15}\n'.format(f'{table}',finaly_csv.shape[0]))
            print(finaly_csv)
        log_file.close()

               

    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )



    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )



    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    
    
    run_bash = BashOperator(
        task_id='run_after_etl',
        bash_command='df -h >> '+outputEtlPath+logFileName)



    extract_task >> transform_task >> load_task >> run_bash







