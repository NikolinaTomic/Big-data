#!/usr/bin/python3

import csv
import os
import time

import kafka.errors
from kafka import KafkaProducer

KAFKA_BROKER = os.environ["KAFKA_BROKER"]

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

with open('Accident.csv') as csv_file:
    with open('Vehicle.csv') as veh_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        veh_reader = csv.reader(veh_file, delimiter=',')
        for acc,veh in zip(csv_reader,veh_reader):
            idAcc = str(acc).split(',')[0]
            year = str(acc).split(',')[11]
            dayOfWeek = str(acc).split(',')[12]
            producer.send("acctopic", key=bytes(idAcc, 'utf-8'), value=bytes(year, 'utf-8'))
            producer.send("acc2topic", key=bytes(idAcc, 'utf-8'), value=bytes(dayOfWeek, 'utf-8'))
            idAccV = str(veh).split(',')[0]
            driverAge = str(veh).split(',')[15]
            producer.send("vehtopic", key=bytes(idAccV, 'utf-8'), value=bytes(driverAge, 'utf-8'))

