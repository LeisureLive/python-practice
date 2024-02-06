#!/bin/env python
# -*- coding: UTF-8 -*-

from kafka import KafkaConsumer


def consumeKafkaData():
    topic = "my_topic"
    consumer = KafkaConsumer(topic, bootstrap_servers='10.129.31.107:9092', group_id="fqj")

    # 持续消费消息
    for message in consumer:
        # 解析消息的主题、分区和偏移量
        topic = message.topic
        partition = message.partition
        offset = message.offset

        # 解析消息的键和值
        key = message.key
        value = message.value

        # 在这里添加你的消费逻辑
        print(f'Topic: {topic}, Partition: {partition}, Offset: {offset}, Key: {key}, Value: {value.decode("utf-8")}')

    print("kafka records consume finished")
    # 关闭 KafkaConsumer 对象
    consumer.close()


if __name__ == "__main__":
    consumeKafkaData()
