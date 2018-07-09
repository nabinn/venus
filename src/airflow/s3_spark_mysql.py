from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta


start_date = datetime(2018, 7, 3, 20, 0, 0, 0)
json_file = "s3://venmo-dataset/unprocessed/venmo_2013_06_01.json"


default_args = {
    'owner': 'nabin',
    'depends_on_past': False,
    'start_date': start_date,
    'email':'imnabn@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# run every hour
my_dag = DAG('s3_spark_mysql', default_args=default_args, schedule_interval='0 * * * *')

t1 = S3KeySensor(
    task_id='s3_file_test',
    poke_interval=30,
    timeout=10,
    soft_fail=False,
    bucket_key=json_file,
    bucket_name=None,
    wildcard_match=True,
    dag=my_dag)


t2 = BashOperator(
    task_id='extract_users',
    depends_on_past=False,
    bash_command="""$SPARK_HOME/bin/spark-submit \
    --master spark://ip-10-0-0-11:7077 \
    --executor-memory 6G \
    /home/ubuntu/venmo/spark/userinfo.py 2013 6 1""",
    dag=my_dag)

'''
t22 = BashOperator(
    task_id='userinfo_df_save',
    depends_on_past=False,
    bash_command="""$SPARK_HOME/bin/spark-submit \
    --packages mysql:mysql-connector-java:5.1.40 \
    --master spark://ip-10-0-0-11:7077 \
    --executor-memory 6G \
    /home/ubuntu/venmo/spark/userinfo_df_save.py 2017 1 2""",
    dag=my_dag)

'''
t3 = BashOperator(
    task_id='net_spending',
    depends_on_past=False,
    bash_command="""$SPARK_HOME/bin/spark-submit \
    --master spark://ip-10-0-0-11:7077 \
    --executor-memory 2G \
    /home/ubuntu/venmo/spark/net_spending.py 2013 6 1""",
    dag=my_dag)


t4 = BashOperator(
    task_id='pair_frequency',
    depends_on_past=False,
    bash_command="""$SPARK_HOME/bin/spark-submit \
    --master spark://ip-10-0-0-11:7077 \
    --executor-memory 2G \
    /home/ubuntu/venmo/spark/pair_frequency.py 2013 6 1""",
    dag=my_dag)


t5 = BashOperator(
    task_id='receive_frequency',
    depends_on_past=False,
    bash_command="""$SPARK_HOME/bin/spark-submit \
    --master spark://ip-10-0-0-11:7077 \
    --executor-memory 1G \
    /home/ubuntu/venmo/spark/receive_frequency.py 2013 6 1""",
    dag=my_dag)

t6 = BashOperator(
    task_id='send_frequency',
    depends_on_past=False,
    bash_command="""$SPARK_HOME/bin/spark-submit \
    --master spark://ip-10-0-0-11:7077 \
    --executor-memory 1G \
    /home/ubuntu/venmo/spark/send_frequency.py 2013 6 1""",
    dag=my_dag)

t7 = BashOperator(
    task_id='mark_as_processed',
    depends_on_past=False,
    trigger_rule=TriggerRule.ALL_DONE,
    bash_command="aws s3 mv s3://venmo-dataset/unprocessed/venmo_2013_06_01.json s3://venmo-dataset/processed/",
    dag=my_dag)

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t2)
t5.set_upstream(t2)
t6.set_upstream(t2)
t7.set_upstream([t3, t4, t5, t6])
