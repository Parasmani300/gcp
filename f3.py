from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import Consumer, KafkaError

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to consume Kafka messages
def consume_kafka_messages():
    consumer_config = {
        'bootstrap.servers': 'your_bootstrap_servers',
        'group.id': 'your_consumer_group_id',
        'auto.offset.reset': 'earliest',  # Start consuming from the beginning of the topic
        'enable.auto.commit': False,     # Disable automatic offset committing
        # Add any other configuration options specific to your setup
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(['your_kafka_topic'])

    try:
        while True:
            message = consumer.poll(1.0)  # Poll for new messages with a timeout of 1 second

            if message is None:
                continue
            elif not message.error():
                # Process the message
                print(f"Received message: {message.value().decode('utf-8')}")

                # Manually commit the offset to mark the message as processed
                consumer.commit(message)
            elif message.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of partition")
            else:
                print(f"Error while consuming: {message.error().str()}")

    except KeyboardInterrupt:
        consumer.close()

# Define the DAG
with DAG(
    'kafka_consumer_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),  # Set the interval between DAG runs
) as dag:

    # Define the task using the PythonOperator
    consume_task = PythonOperator(
        task_id='consume_kafka_messages_task',
        python_callable=consume_kafka_messages,
    )

# You can define other tasks and dependencies here if needed
