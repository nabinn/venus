
"""
This script uses transaction data to calculate the net amout spent by a user
on a particular day (assuming net_spending = amount_sent - amount_received)
and saves it to mysql table called net_spending
"""

from pyspark import SparkContext
from utils import get_url
from utils import sql_create_table, sql_insert_rdd_to_table
import json
import sys
import datetime
import time
import random
from dateutil import parser
import logging
from utils import get_logfile_name

logging.basicConfig(filename=get_logfile_name(__file__),
                    level=logging.INFO,
                    format='%(asctime)s %(message)s')


def parse_data(record):
    try:
        json_body = json.loads(record)
        sender_id = json_body['actor']['id']
        receiver_id = json_body['transactions'][0]['target']['id']

        timestamp = json_body['created_time']
        transaction_date = parser.parse(timestamp).date()

        amount = random.randint(1, 99999)

        return [((sender_id, transaction_date), amount), ((receiver_id, transaction_date), -amount)]
    except:
        return [None, None]


create_table_net_spending = """CREATE TABLE IF NOT EXISTS net_spending(
                                user_id INT NOT NULL,
                                transaction_date DATE,
                                amount DECIMAL,
                                PRIMARY KEY (user_id, transaction_date),
                                FOREIGN KEY (user_id) REFERENCES users(id)
                                );
					"""

add_net_spending = """INSERT IGNORE INTO net_spending(user_id, transaction_date, amount) VALUES (%s,%s,%s);"""

if __name__ == '__main__':

    sc = SparkContext(appName="venmoApp-net-spending")
    data_location = get_url(sys.argv)

    if data_location is None:
        logging.error("not a valid data location.\nExiting the program")
        sys.exit(0)

    logging.info("Processing: " + data_location)
    data_rdd = sc.textFile(data_location)

    parsed_spenders = data_rdd.flatMap(parse_data). \
        filter(lambda data: data is not None). \
        reduceByKey(lambda a, b: a + b). \
        map(lambda rdd: (rdd[0][0], rdd[0][1], rdd[1]))

    table_created = sql_create_table(create_table_net_spending)

    if table_created:
        start_time = time.time()
        data_inserted = sql_insert_rdd_to_table(
            prepared_statement=add_net_spending,
            collected_rdd=parsed_spenders.collect()
        )

        if data_inserted:
            end_time = time.time()
            logging.info("Processed " + str(parsed_spenders.count()) + " rows in " +
                         str(end_time - start_time) + " seconds\n")
        else:
            logging.error("Error while inserting to table")
            sys.exit(1)

    else:
        logging.error("Error in table creation")
        sys.exit(1)
