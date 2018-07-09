"""
Script to extract user information from transactions and save to mysql database
using Spark Dataframe's write api. This approach is faster compared to writing collected
rdd using mysql connector.
"""

from pyspark.sql import SparkSession
from utils import get_url, db_config
from utils import sql_create_table, sql_run_custom_query
import json
import sys
import time
import logging
from utils import get_logfile_name
logging.basicConfig(filename=get_logfile_name(__file__),
                    level=logging.INFO,
                    format='%(asctime)s %(message)s')


def parse_user_info(json_record):
    """takes in a json record and returns a
    list of(user_id, username, firstname, lastname, image)
    of sender and receiver"""
    try:
            json_body = json.loads(json_record)

            # Sender data
            sender_id = int(json_body['actor']['id'])
            sender_firstname = json_body['actor']['firstname']
            sender_lastname = json_body['actor']['lastname']
            sender_username = json_body['actor']['username']
            sender_image = json_body['actor']['picture']
            # Receiver data
            receiver_id = int(json_body['transactions'][0]['target']['id'])
            receiver_firstname = json_body['transactions'][0]['target']['firstname']
            receiver_lastname = json_body['transactions'][0]['target']['lastname']
            receiver_username = json_body['transactions'][0]['target']['username']
            receiver_image = json_body['transactions'][0]['target']['picture']
            return [
            (sender_id, sender_username, sender_firstname, sender_lastname, sender_image),
            (receiver_id, receiver_username, receiver_firstname, receiver_lastname, receiver_image)
            ]
    except:
            return [None, None]


create_users_table_stmt = """CREATE TABLE IF NOT EXISTS users(
                                id INT PRIMARY KEY,
                                username VARCHAR(50) NOT NULL,
                                firstname VARCHAR(100),
                                lastname VARCHAR(100),
                                picture TEXT
                                );
					"""


if __name__=="__main__":

    spark = SparkSession.builder.appName('venmoApp-userinfo-df').getOrCreate()
    sc = spark.sparkContext
    #sc = SparkContext(appName="venmoApp-userinfo-mysql")

    data_location = get_url(sys.argv)

    if data_location is None:
        logging.error("not a valid data location.\nExiting the program")
        sys.exit(1)

    logging.info("Processing:"+ data_location+" using dataframe and write")

    start_time = time.time()
    data_rdd = sc.textFile(data_location)

    parsed_users = data_rdd.flatMap(parse_user_info).\
                filter(lambda data: data is not None)

    users_df = spark.createDataFrame(parsed_users,
                            ["id", "username", "firstname", "lastname", "picture"])

    distinct_users_df=users_df.dropDuplicates(["id"])
    distinct_users_df.write.format('jdbc').options(
          url='jdbc:mysql://'+db_config["host"]+"/"+db_config["database"],
          driver='com.mysql.jdbc.Driver',
          dbtable='users_temp',
          user=db_config["user"],
          password=db_config["password"]).mode("overwrite").save() #mode("append")
    end_time=time.time()

    logging.info("Processed "+str(distinct_users_df.count())+" users in "+
         str(end_time-start_time)+ " seconds\n")

    #after writing dataframe to temp table perform
    # upsert operation on users table.
    start_time=time.time()
    # create table if not exists
    #sql_create_table(create_users_table_stmt)
    # copy data from users_temp to users by removing dropDuplicates
    sql_run_custom_query("""INSERT IGNORE INTO users
                         SELECT id, username, firstname, lastname, picture
                         FROM users_temp;
                         """)
    end_time=time.time()
    logging.info("Upsert into database took "+
         str(end_time-start_time)+ " seconds\n")
