from __future__ import print_function
from pyspark import SparkContext
from utils import get_url
from utils import sql_create_table, sql_insert_rdd_to_table
import json
import sys
import time
import logging

logging.basicConfig(filename='/home/ubuntu/venmo/logs/userinfo.log',
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
add_user_stmt="""INSERT IGNORE INTO users(id, username, firstname, lastname, picture)
		VALUES (%s,%s,%s,%s, %s);"""


if __name__=="__main__":

    sc = SparkContext(appName="venmoApp-userinfo-mysql")

    data_location = get_url(sys.argv)

    if data_location is None:
        print("not a valid data location.\nExiting the program")
        sys.exit(0)
    logging.info("Processing:"+ data_location)

    data_rdd = sc.textFile(data_location)

    parsed_users = data_rdd.flatMap(parse_user_info).\
                filter(lambda data: data is not None)

    table_created = sql_create_table(create_users_table_stmt)

    if table_created:
        start_time=time.time()
        data_inserted = sql_insert_rdd_to_table(
                        prepared_statement=add_user_stmt,
                        collected_rdd=parsed_users.collect()
                    )

        if data_inserted:
            end_time=time.time()
            logging.info("Processed "+str(parsed_users.count())+" users in "+
                str(end_time-start_time)+ " seconds\n")
        else:
            print("Error while inserting to table")
            sys.exit(1)

    else:
        print("Error in table creation")
        sys.exit(1)
