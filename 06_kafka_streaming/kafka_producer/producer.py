import datetime

import requests
from sports_api import  Sportsapi
from confluent_kafka import Producer

# Параметры подключения к Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'sports_games'


# Функция отправки сообщений в Kafka
def produce_messages(data):
    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

    # Отправка сообщения в Kafka
    producer.produce(kafka_topic, key=None, value=data)

    # Ожидание подтверждения от Kafka
    producer.flush()

    print('Сообщение отправлено в Kafka: {}'.format(data))


# Получение данных из API и отправка их в Kafka
def fetch_data_and_produce():
    api_url = Sportsapi.get_tour_calendar(datetime.datetime.now())

    response = requests.get(api_url)

    if response.status_code == 200:
        games = response.json()

        for game in games:
            # Пример преобразования данных перед отправкой в Kafka
            transformed_data = transform_data(game)

            # Отправка данных в Kafka
            produce_messages(transformed_data)
    else:
        print('Ошибка при получении данных из API.')


# Пример преобразования данных перед отправкой в Kafka
def transform_data(game):
    # Возможно, здесь нужно преобразовать или отфильтровать данные перед отправкой в Kafka
    transformed_data = {
        'id': game['id'],
        'team1': game['team1'],
        'team2': game['team2'],
        'score': game['score']
    }
    return transformed_data


# Запуск процесса получения данных из API и отправки в Kafka
fetch_data_and_produce()
