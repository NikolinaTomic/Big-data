r"""
Run the example
docker exec -it spark-master bash
spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 /consumer/avgVehAge.py zoo1:2181 veh2topic
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sum = 0
count = 0


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


def calculateAvg(rdd):
    global sum
    global count
    try:
        if str(rdd.collect()) != '[]':
            age = rdd.take(1).pop()
            age = str(age).replace("'", "")
            age = int(age)
            sum += age
            count += 1
            avg = round((float(sum) / float(count)), 3)
            print("Average: " + str(avg) + " years")
    except:
        print("except")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: avgVehAge.py <zk> <topic>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="SparkStreaming")
    quiet_logs(sc)
    ssc = StreamingContext(sc, 4)

    zooKeeper, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zooKeeper, "spark-streaming-consumer", {topic: 1})
    ssc.checkpoint("stateful_checkpoint_direcory")
    avg = kvs.map(lambda x: x[1]).filter(lambda age: not ("-1" in age)).foreachRDD(lambda rdd: calculateAvg(rdd))

    print("\nAverage age of vehicle in car accidents:")

    ssc.start()
    ssc.awaitTermination()
