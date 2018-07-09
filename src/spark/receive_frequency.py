"""
Script to aggregate result on the number of times a user receives money per day.
The aggregated result is saved to the receiver_activity table.
"""

from pyspark import SparkContext
from utils import get_url
from utils import sql_create_table, sql_insert_rdd_to_table
import json
import sys
import datetime
import time
from dateutil import parser
import logging
from utils import get_logfile_name
logging.basicConfig(filename=get_logfile_name(__file__),
                    level=logging.INFO,
                    format='%(asctime)s %(message)s')

def get_receiver(json_record):
    """maps a json record to ((receiver_id, transaction_date), 1)"""
    try:
            json_body = json.loads(json_record)
            receiver_id = int(json_body['transactions'][0]['target']['id'])
            timestamp = json_body['created_time']
            transation_date = parser.parse(timestamp).date()
            return ((receiver_id, transation_date), 1)
    except:
            return None


create_table_receiver_activity = """CREATE TABLE IF NOT EXISTS receiver_activity(
                                user_id INT NOT NULL,
                                transaction_date DATE,
                                recv_freq INT,
                                PRIMARY KEY(user_id, transaction_date),
                                FOREIGN KEY (user_id) REFERENCES users(id)
                                );
					"""


add_receiver_activity="""INSERT IGNORE INTO receiver_activity(user_id, transaction_date, recv_freq)
                            VALUES (%s,%s,%s);"""



if __name__ == '__main__':

    sc = SparkContext(appName="venmoApp-receiver-activity")

    data_location = get_url(sys.argv)

    if data_location is None:
        logging.error("not a valid data location.\nExiting the program")
        sys.exit(1)

    logging.info("Processing: "+data_location)
    data_rdd = sc.textFile(data_location)

    parsed_receivers = data_rdd.map(get_receiver).\
                    filter(lambda data:data is not None).\
                    reduceByKey(lambda a,b:a+b).\
                    map(lambda rdd: (rdd[0][0], rdd[0][1], rdd[1]))

    table_created = sql_create_table(create_table_receiver_activity)

    if table_created:
        start_time=time.time()

        data_inserted = sql_insert_rdd_to_table(
                        prepared_statement=add_receiver_activity,
                        collected_rdd=parsed_receivers.collect()
                    )
        if data_inserted:
            end_time=time.time()
            logging.info("Processed "+str(parsed_receivers.count())+" users in "+
                str(end_time-start_time)+ " seconds\n")
        else:
            logging.error("Error while inserting")
            sys.exit(1)

    else:
        logging.error("Error in table creation")
        sys.exit(1)
