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

with open('Vehicle.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for veh in csv_reader:
        idAcc = str(veh).split(',')[0]
        ageOfVeh = str(veh).split(',')[19]
        producer.send("veh2topic", key=bytes(idAcc, 'utf-8'), value=bytes(ageOfVeh, 'utf-8'))
