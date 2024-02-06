from kafka import KafkaProducer


def sendDataToKafka():
    # 创建 KafkaProducer 对象，指定 Kafka 服务器的地址和端口
    producer = KafkaProducer(bootstrap_servers='10.129.31.107:9092', acks="all")

    # 发送消息到指定的主题
    topic = 'my_topic'  # 替换为你要发送的主题名称
    for i in range(100):
        # 将消息转换为字节流并发送
        message = str(i)
        print(message)
        producer.send(topic, value=message.encode("UTF-8"))

    producer.flush()
    # 关闭 KafkaProducer 对象
    producer.close()


if __name__ == "__main__":
    sendDataToKafka()
