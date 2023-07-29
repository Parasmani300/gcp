from kafka import KafkaConsumer
import json

def getKafkaConsumer(config,isLocalCluster):
    bootstrapServer=config['bootstrapServer']
    consumer = None
    if(isLocalCluster):
        consumer = KafkaConsumer(
            bootstrap_servers =[bootstrapServer],
            group_id='my-grp',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
    else:
        api_key = config['api_key']
        api_password= config['api_password']
        consumer = KafkaConsumer(
            bootstrap_servers = [bootstrapServer],
            group_id='my-grp',
            security_protocol='SASL_SSL',
            sasl_mechanism ='PLAIN',
            sasl_plain_username =api_key,
            sasl_plain_password =api_password,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
    return consumer

def startMessageConsumption(topic,consumer:KafkaConsumer):
    consumer.subscribe(topics=[topic])
    for message in consumer:
        print(message.value)

if __name__ == "__main__":
    config:dict = {
        'api_key':'ZCLIIIHGVZ6NHGOE',
        'api_password':'A8n/sRBpKb8weCSZjkjuh4wgHayF+5aVJ4uLIF8TYdqJQBfyOXuhy+FLPA9nI62d',
        'bootstrapServer':'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
    }
    topic:str = 'demo_topic'
    consumer = getKafkaConsumer(config=config,isLocalCluster=False)

    startMessageConsumption(topic,consumer)
    
