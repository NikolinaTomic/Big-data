r"""
Run the example
docker exec -it spark-master bash
spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 /consumer/youngDrivers.py zoo1:2181 vehtopic
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

allDrivers = 0
youngDrivers = 0


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


def calculatePercentage(rdd):
    global allDrivers
    global youngDrivers
    try:
        if str(rdd.collect()) != '[]':
            age = rdd.take(1).pop()
            age = str(age).replace("'", "")
            age = int(age)
            if age < 30:
                youngDrivers += 1
                allDrivers += 1
            else:
                allDrivers += 1
            procenat = round((float(youngDrivers * 100) / float(allDrivers)), 3)
            print("Percentage: " + str(procenat) + "%")
    except:
        print("except")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: youngDrivers.py <zk> <topic>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="SparkStreaming")
    quiet_logs(sc)
    ssc = StreamingContext(sc, 4)

    zooKeeper, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zooKeeper, "spark-streaming-consumer", {topic: 1})
    ssc.checkpoint("stateful_checkpoint_direcory")
    young = kvs.map(lambda x: x[1])

    counts = young.foreachRDD(lambda rdd: calculatePercentage(rdd))

    print("\nPercentage of young drivers (under 30 years) in car accidents:")

    ssc.start()
    ssc.awaitTermination()
