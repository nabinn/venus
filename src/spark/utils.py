'''contains some utility functions and configurations'''
from __future__ import print_function
import mysql.connector

db_config={
	'host':'host_ip',
	'user':'db_user_name',
	'password':'db_password',
	'database':'db_name'
}


def get_url(argv, s3_bucket_name="venmo-json"):
    '''provides url for data based on argv'''

    if len(argv) == 2:
        year = argv[1]
        return "s3a://"+s3_bucket_name+"/"+str(year)+"_*"

    elif len(argv) == 3:
        year = argv[1]
        month = argv[2]
        return ("s3a://"+s3_bucket_name+"/"+
            str(year)+"_"+str(month).zfill(2)+"/*")

    elif len(argv) == 4:
        year = argv[1]
        month = argv[2]
        day = argv[3]
        return ("s3a://"+s3_bucket_name+"/"+
            str(year)+"_"+str(month).zfill(2)+"/venmo_"+
            str(year)+"_"+str(month).zfill(2)+"_"+str(day).zfill(2)+".json")
    else:
        return None


def sql_create_table(sql_create_table_statement):
	''' connects to the database and creates table if it does not exist
		based on sql_create_table_statement.
	'''
	try:
		connection = mysql.connector.connect(**db_config)
		cursor = connection.cursor()

		cursor.execute(sql_create_table_statement)

		connection.commit()
		cursor.close()
		connection.close()
		return True

	except Exception as e:
		print(e)
		return False


def sql_insert_rdd_to_table(prepared_statement, collected_rdd):
	''' uses prepared statement to insert collected rdd to table'''
	try:
		connection = mysql.connector.connect(**db_config)
		cursor = connection.cursor()

		cursor.executemany(prepared_statement, collected_rdd)

		connection.commit()
		cursor.close()
		connection.close()
		return True

	except Exception as e:
		print(e)
		return False
