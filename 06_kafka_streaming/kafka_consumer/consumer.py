from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# Параметры подключения к Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'sports_games'
kafka_group_id = 'sports_consumer'

# Параметры подключения к MongoDB
mongodb_uri = "mongodb://localhost:27017/"
database_name = "euro-stat"
collection_name = "sport_collect"


# Функция обработки сообщений из Kafka
def consume_messages():
    consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': kafka_group_id,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([kafka_topic])

    client = MongoClient(mongodb_uri)
    db = client[database_name]
    collection = db[collection_name]

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print('Ошибка при чтении сообщения из Kafka: {}'.format(msg.error()))
                    break

            # Получение данных из сообщения
            message_value = msg.value().decode('utf-8')

            # Пример обработки данных перед записью в MongoDB
            processed_data = process_data(message_value)

            # Запись обработанных данных в MongoDB
            collection.insert_one(processed_data)

            print('Записано в MongoDB: {}'.format(processed_data))

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


# Пример обработки данных перед записью в MongoDB
def process_data(data):
    # Возможно, здесь нужно преобразовать данные или произвести другую обработку перед записью в MongoDB
    processed_data = {
        'value': data
    }
    return processed_data


# Запуск потребителя (consumer)
consume_messages()
