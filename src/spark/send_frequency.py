"""Script to aggregate and save data on how many times does a user sends money on a daily basis"""

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
logging.basicConfig(filename='/home/ubuntu/venmo/logs/send_frequency.log',
                    level=logging.INFO,
                    format='%(asctime)s %(message)s')

def get_sender(json_record):
    """maps each json record to ( (sender_id, transaction_date), 1) """
    try:
            json_body = json.loads(json_record)
            sender_id = int(json_body['actor']['id'])
            timestamp = json_body['created_time']
            transation_date = parser.parse(timestamp).date()
            return ((sender_id, transation_date), 1)
    except:
            return None

create_table_sender_activity = """CREATE TABLE IF NOT EXISTS sender_activity(
                                user_id INT NOT NULL,
                                transaction_date DATE,
                                send_freq INT,
                                PRIMARY KEY(user_id, transaction_date),
                                FOREIGN KEY (user_id) REFERENCES users(id)
                                );
					"""


add_sender_activity="""INSERT IGNORE INTO sender_activity(user_id, transaction_date, send_freq)
                            VALUES (%s,%s,%s);"""


if __name__ == '__main__':

    sc = SparkContext(appName="venmoApp-sender-activity")

    data_location = get_url(sys.argv)

    if data_location is None:
        print("not a valid data location.\nExiting the program")
        sys.exit(0)

    logging.info("Processing: "+data_location)

    data_rdd = sc.textFile(data_location)

    parsed_senders = data_rdd.map(get_sender).\
                filter(lambda data: data is not None).\
                reduceByKey(lambda a,b: a+b).\
                map(lambda rdd: (rdd[0][0], rdd[0][1], rdd[1]))


    table_created = sql_create_table(create_table_sender_activity)

    if table_created:
        start_time=time.time()

        data_inserted = sql_insert_rdd_to_table(
                        prepared_statement=add_sender_activity,
                        collected_rdd=parsed_senders.collect()
                    )

        if data_inserted:
            end_time=time.time()
            logging.info("Processed "+str(parsed_senders.count())+" users in "+
                str(end_time-start_time)+ " seconds\n")
        else:
            print("Error while inserting")
            sys.exit(0)

    else:
        print("Error in table creation")
        sys.exit(0)
