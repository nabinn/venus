"""Extract user information from transactions and saves to mysql database
using dataframe.save('jdbc')
"""

from __future__ import print_function
from pyspark.sql import SparkSession
from utils import get_url
import json
import sys
import time
import logging
logging.basicConfig(filename='/home/ubuntu/venmo/logs/userinfo_df_save.log',
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


if __name__=="__main__":

    spark = SparkSession.builder.appName('venmoApp-userinfo-mysql').getOrCreate()
    sc = spark.sparkContext
    #sc = SparkContext(appName="venmoApp-userinfo-mysql")

    data_location = get_url(sys.argv)

    if data_location is None:
        print("not a valid data location.\nExiting the program")
        sys.exit(0)

    logging.info("Processing:"+ data_location+" using dataframe and write")

    start_time = time.time()
    data_rdd = sc.textFile(data_location)

    parsed_users = data_rdd.flatMap(parse_user_info).\
                filter(lambda data: data is not None)

    users_df = spark.createDataFrame(parsed_users,
                            ["id", "username", "firstname", "lastname", "picture"])

    distinct_users_df=users_df.dropDuplicates(["id"])
    distinct_users_df.write.format('jdbc').options(
          url='jdbc:mysql://10.0.0.14/venmo_db',
          driver='com.mysql.jdbc.Driver',
          dbtable='users_temp',
          user='nabin',
          password='123').mode('append').save()
    end_time=time.time()

    logging.info("Processed "+str(distinct_users_df.count())+" users in "+
         str(end_time-start_time)+ " seconds\n")
