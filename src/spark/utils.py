'''Utility functions and configurations'''
import mysql.connector
import os

PROJECT_DIR="/home/ubuntu/venmo/"

db_config={
	'host':'host_ip',
	'user':'username',
	'password':'password',
	'database':'database_name'
}

def get_logfile_name(file_name, logdir=PROJECT_DIR+"logs/"):
        base_name = os.path.basename(file_name)
        log_fname=os.path.splitext(base_name)[0]+".log"
        return logdir+log_fname

def get_url(argv, mode="date"):
    '''Provides url for data based on argv and mode.

	   mode: file or date or temp
	   		date mode: expects date in [year month day] format as argv
			file mode: expects file location in s3 bucket
			temp mode: expects file location in a temporary s3 bucket
	'''
    if mode=="date":
		s3_bucket_name="venmo-json"
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

    elif mode=="file":
        return argv

    elif mode=="temp":
        s3_bucket_name="venmo-dataset"
        return "s3a://"+s3_bucket_name+"/unprocessed/venmo_"+\
		str(argv[1])+"_"+str(argv[2]).zfill(2)+"_"+str(argv[3]).zfill(2)+".json"

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
		return False


def sql_run_custom_query(custom_query):
	''' connects to the database and runs custom sql query passed as argument
	'''
	try:
		connection = mysql.connector.connect(**db_config)
		cursor = connection.cursor()

		cursor.execute(custom_query)

		connection.commit()
		cursor.close()
		connection.close()
		return True

	except Exception as e:
		return False
