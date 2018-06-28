from __future__ import print_function
from pyspark import SparkContext
from utils import get_url
from utils import sql_create_table, sql_insert_rdd_to_table
import json
import sys
import datetime
import time
from dateutil import parser
import logging
logging.basicConfig(filename='/home/ubuntu/venmo/logs/pair_frequency.log',
                    level=logging.INFO,
                    format='%(asctime)s %(message)s')


def parse_pairs(json_record):
    try:
            json_body = json.loads(json_record)
            # Sender data
            sender_id = int(json_body['actor']['id'])
            # Receiver data
            receiver_id = int(json_body['transactions'][0]['target']['id'])
            timestamp = json_body['created_time']
            transaction_date = parser.parse(timestamp).date()

            if sender_id < receiver_id:
                return ((sender_id, receiver_id, transaction_date), 1)
            else:
                return ((receiver_id, sender_id, transaction_date), 1)
    except:
            return None


create_table_pair_activity = """CREATE TABLE IF NOT EXISTS pair_activity(
                                user1 INT NOT NULL,
                                user2 INT NOT NULL,
                                transaction_date DATE,
                                transaction_freq INT,
                                PRIMARY KEY (user1, user2, transaction_date),
                                FOREIGN KEY (user1) REFERENCES users(id),
                                FOREIGN KEY (user2) REFERENCES users(id)
                                );
					"""

add_pair_activity="""INSERT INTO pair_activity(user1, user2, transaction_date, transaction_freq) VALUES (%s,%s,%s,%s);"""



if __name__=="__main__":

    sc = SparkContext(appName="venmoApp-pair-activity")

    data_location = get_url(sys.argv) # get url of data based on command line args

    if data_location is None:
        print("not a valid data location.\nExiting the program")
        sys.exit(0)

    logging.info("Processing: "+data_location)
    data_rdd = sc.textFile(data_location)

    user_pairs = data_rdd.map(parse_pairs).\
                            filter(lambda data: data is not None).\
                            reduceByKey(lambda a,b: a+b).\
                            map(lambda rdd: (rdd[0][0], rdd[0][1], rdd[0][2], rdd[1]))

    table_created = sql_create_table(create_table_pair_activity)

    if table_created:
        start_time=time.time()
        data_inserted = sql_insert_rdd_to_table(
                        prepared_statement=add_pair_activity,
                        collected_rdd=user_pairs.collect()
                    )

        if data_inserted:
            end_time=time.time()
            logging.info("Processed "+str(user_pairs.count())+" pair of users in "+
                str(end_time-start_time)+ " seconds\n")
        else:
            print("Error while inserting to table")
            sys.exit(1)

    else:
        print("Error in table creation")
        sys.exit(1)
