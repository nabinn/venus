# VenUS: Venmo Usage Statistics 
An automatic data processing pipeline for venmo transactions.

# Project Description
The aim of the project is to rank Venmo users on a daily basis based on their activity .i.e. frequency of transaction and amount spent. 

Following 4 metrics are calculated every day:
 1. Number of times a user sends money
 2. Number of times a user receives money
 3. Numer of times a pair of uesrs take part in a transaction
 4. The amount a user spends per day (i.e. amount sent - amount received)
  
# Data Pipeline
![](/img/pipeline.png)

The data is present in s3 bucket. Spark reads the data and calculates the aggregated result. The result is then saved to MySQL database. The user can then query the database via a frontend that is implemented using Flask. 

The entire process is automated using Airflow such that whenever there is new data in the bucket, spark jobs are triggered and the results are saved to the database. The application also allows for the monitoring of the workflow and notify the user in case of failure.

# Links
- [Demo](https://www.youtube.com/watch?v=FDVOBDm1lcw)
- [PPT](https://docs.google.com/presentation/d/1FUp8HuKw7pjzcJy6l3H62fRjBZxd6bQRZB_IEADHrpY/edit?usp=sharing)
