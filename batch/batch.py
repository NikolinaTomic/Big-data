#!/usr/bin/python
### before spark-submit: export PYTHONIOENCODING=utf8

#Put data on hdfs
#docker exec -it namenode bash
#hdfs dfs -mkdir myData
#hdfs dfs -put /myData /myData

#Run batch.py
#docker exec -it spark-master bash
#spark/bin/spark-submit mySpark/batch.py

import pyspark.sql.functions as func
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


conf = SparkConf().setAppName("uni").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

from pyspark.sql.types import *

########################################ŠEME##############################################################
schemaAcc = "Accident_Index Location_Easting_OSGR Location_Northing_OSGR Longitude Latitude Police_Force \
                Accident_Severity Number_of_Vehicles Number_of_Casualties Day Month Year Day_of_Week Time \
                Local_Authority_(District) Local_Authority_(Highway) 1st_Road_Class 1st_Road_Number \
		Road_Type Speed_limit Junction_Detail Junction_Control 2nd_Road_Class 2nd_Road_Number \
                Pedestrian_Crossing-Human_Control Pedestrian_Crossing-Physical_Facilities Light_Conditions \
		Weather_Conditions Road_Surface_Conditions Special_Conditions_at_Site Carriageway_Hazards \
		Urban_or_Rural_Area Did_Police_Officer_Attend_Scene_of_Accident LSOA_of_Accident_Location"
fieldsAcc = [StructField(field_name, StringType(), True) for field_name in schemaAcc.split()]
schemaAcc = StructType(fieldsAcc)
dfAcc = spark.read.csv("hdfs://namenode:9000/myData/Accident.csv", header=True, mode="DROPMALFORMED",
                       schema=schemaAcc)

schemaVeh = "Accident_Index Vehicle_Reference Vehicle_Type Towing_and_Articulation Vehicle_Manoeuvre Vehicle_Location-Restricted_Lane \
                Junction_Location Skidding_and_Overturning Hit_Object_in_Carriageway Vehicle_Leaving_Carriageway Hit_Object_off_Carriageway \
                1st_Point_of_Impact Was_Vehicle_Left_Hand_Drive? Journey_Purpose_of_Driver Sex_of_Driver \
		Age_of_Driver Age_Band_of_Driver Engine_Capacity_(CC) Propulsion_Code Age_of_Vehicle Driver_IMD_Decile Driver_Home_Area_Type"
fieldsVeh = [StructField(field_name, StringType(), True) for field_name in schemaVeh.split()]
schemaVeh = StructType(fieldsVeh)
dfVeh = spark.read.csv("hdfs://namenode:9000/myData/Vehicle.csv", header=True, mode="DROPMALFORMED",
                       schema=schemaVeh)

schemaCas = "Accident_Index Vehicle_Reference Casualty_Reference Casualty_Class Sex_of_Casualty Age_of_Casualty \
                Age_Band_of_Casualty Casualty_Severity Pedestrian_Location Pedestrian_Movement Car_Passenger Bus_or_Coach_Passenger \
                Pedestrian_Road_Maintenance_Worker Casualty_Type Casualty_Home_Area_Type"
fieldsCas = [StructField(field_name, StringType(), True) for field_name in schemaCas.split()]
schemaCas = StructType(fieldsCas)
dfCas = spark.read.csv("hdfs://namenode:9000/myData/Casualty.csv", header=True, mode="DROPMALFORMED",
                       schema=schemaCas)

schemaHome = "code label"
fieldsHome = [StructField(field_name, StringType(), True) for field_name in schemaHome.split()]
schemaHome = StructType(fieldsHome)
dfHome = spark.read.csv("hdfs://namenode:9000/myData/Home_area_type.csv", header=True, mode="DROPMALFORMED",
                        schema=schemaHome)
###########################################################################################################

#Number of accidents per year
dfAccCounted = dfAcc.groupBy('Year').count().sort("Year") \
    .withColumnRenamed("count", "Number of accidents")

dfAccCounted.coalesce(1).write.format("csv").save("/results/numOfAccidentsPerYear.csv")
print(" \nNumber of accidents per year (2005 - 2015):")
dfAccCounted.show()

#Percentage of young drivers in car accidents
dfVeh.createOrReplaceTempView("vehicle")
sqlDF = spark.sql("SELECT (SELECT COUNT(*) FROM vehicle  WHERE Age_of_Driver<30)/COUNT(*) FROM vehicle")
dfPercOfYoungDrivers = sqlDF.toDF("Percentage")
dfPercOfYoungDrivers = dfPercOfYoungDrivers \
  .withColumn("Percentage", func.round(dfPercOfYoungDrivers["Percentage"], 2))
dfPercOfYoungDrivers=dfPercOfYoungDrivers \
  .withColumn("Percentage", dfPercOfYoungDrivers.Percentage * 100) \
  .withColumn('Percentage', sf.concat(sf.col('Percentage'), sf.lit('%')))

dfPercOfYoungDrivers.coalesce(1).write.format("csv").save("/results/percentageOfYoungDrivers.csv")
print(" \nPercentage of young drivers (under 30 years) in car accidents:")
dfPercOfYoungDrivers.show()

#Average age of vehicle in car accidents
dfVeh.createOrReplaceTempView("vehAge")
sqlDF = spark.sql("SELECT SUM(Age_of_Vehicle)/COUNT(Age_of_Vehicle) FROM vehAge")
sumAgeVeh = sqlDF.toDF("Avg")
sumAgeVeh=sumAgeVeh.withColumn("Avg", func.round(sumAgeVeh["Avg"], 3)) \
  .withColumn('Avg', sf.concat(sf.col('Avg'), sf.lit(' years')))

sumAgeVeh.coalesce(1).write.format("csv").save("/results/avgAgeOfVehicle.csv")
print(" \nAverage age of vehicle in car accidents:")
sumAgeVeh.show()

#Percentage of fatal outcomes in car accidents depending on the type of area in which accident has happened
dfCasGroupedFatal = dfCas.filter(dfCas['Casualty_Severity'] == 1).groupBy('Casualty_Home_Area_Type').count()
dfCasGrouped = dfCas.groupBy('Casualty_Home_Area_Type').count()
joinedFatalAll = dfCasGroupedFatal.join(dfCasGrouped, \
  dfCasGroupedFatal.Casualty_Home_Area_Type == dfCasGrouped.Casualty_Home_Area_Type, how="full") \
  .select(dfCasGroupedFatal['Casualty_Home_Area_Type'],(dfCasGroupedFatal['count'] / dfCasGrouped['count']) \
  .alias('Percentage'))

joinedFatalHome = joinedFatalAll.join(dfHome, dfHome.code == joinedFatalAll.Casualty_Home_Area_Type, how="full") \
  .sort('Percentage', ascending=False)
joinedFatalHome = joinedFatalHome.withColumnRenamed("label", "Area type") \
  .select("Area type","Percentage")

joinedFatalHome = joinedFatalHome.withColumn("Percentage", func.round(joinedFatalHome["Percentage"], 4))
joinedFatalHome = joinedFatalHome.withColumn("Percentage", joinedFatalHome.Percentage * 100) \
  .withColumn('Percentage', sf.concat(sf.col('Percentage'), sf.lit('%')))

joinedFatalHome.coalesce(1).write.format("csv").save("/results/percentageOfFatalOutcomesPerArea.csv")
print(" \nPercentage of fatal outcomes in car accidents depending on the type of area in which accident has happened:")
joinedFatalHome.show()
