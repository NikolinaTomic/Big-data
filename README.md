# Big data
Application for batch and stream processing large amount of data using Apache Spark and Kafka

Dataset used in this project is in the csv format (about 1GB large). It is about car accidents that happened in the UK from 2005 to 2015. Some of the answered questions in this project are:
  - Number of accidents sorted by year 
  - Percentage of young drivers (under 30 years) in car accidents
  - Average age of vehicle in car accidents
  - Percentage of fatal outcomes in car accidents depending on the type of area in which accident has happened (possible types of area are Urban area, small town, Rural area or data missing)

# Steps
**1. Unzip csv files in myData, producer and producer2 directories**

**2. Build and start project in docker using following commands**
```sh
$ docker-compose build 
$ docker-compose up
```
**3. Copy myData directory to HDFS using following commands:**
```sh
$ docker exec -it namenode bash 
$ hdfs dfs -mkdir myData
$ hdfs dfs -put /myData /myData
$ exit
```
**4. Start batch processing**
```sh
$ docker exec -it spark-master bash
$ spark/bin/spark-submit mySpark/batch.py
```
**or with**
```sh
$ docker exec spark-master spark/bin/spark-submit mySpark/batch.py
```
Results of batch processing is going to be written in terminal and in the hdfs files in the results directory.
You can access these files in 2 ways:
  - Access Hue's web page with: localhost:8888
  - Login to namenode:
```sh   
docker exec -it namenode bash 
hdfs dfs -ls /results/
```
**4. Stream processing**
Stream processing task is divided into 2 seperate files. You need to run them both using following commands.
First in botj cases:
```sh   
$ docker exec -it spark-master bash
```
Then
```sh   
$ spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 /consumer/youngDrivers.py zoo1:2181 vehtopic
```
```sh   
$ spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 /consumer/avgVehAge.py zoo1:2181 veh2topic
```
Stream processing results is going to be written and updated in terminal on 4 seconds.
**5. Shut down all containers**
```sh   
$ docker-compose down
```
# Notes
You can always see logs of all running containers
```sh
$ docker logs -f {CONTAINER_ID}|{CONTAINER_NAME}
```
If you get error message saying that namenode is in safe mode do the following command:
```sh   
$ docker exec namenode hadoop dfsadmin -safemode leave
```


