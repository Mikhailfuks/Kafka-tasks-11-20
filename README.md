tasks 11 
  import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamsExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("stream-topic");
        stream.mapValues(value -> "Processed: " + value)
              .to("processed-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
tasks 12 

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

public class KafkaFlinkWordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-consumer");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>("flink-topic", new SimpleStringSchema(), props));

        // Word count
        DataStream<Tuple2<String, Integer>> wordCounts = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split("\s+");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        })
        .keyBy(0)
        .sum(1);

        // Kafka Sink
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-producer");
        wordCounts.addSink(new FlinkKafkaProducer<>("flink-wordcount-topic", new SimpleStringSchema(), props));

        env.execute("Kafka Flink WordCount");
    }
}

tasks 13 

from kafka import KafkaProducer
from kafka.errors import KafkaError
import ssl

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
      security_protocol='SSL',
     ssl_context=ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH),
     ssl_cafile='/path/to/ca.pem',  # Сертификат CA
    ssl_certfile='/path/to/client.cert.pem', # Сертификат клиента
    ssl_keyfile='/path/to/client.key.pem')  # Ключ клиента

producer.send('my-topic', b'Hello, Secure Kafka!')
producer.flush()

tasks 14 

package main

import (
 "context"
 "fmt"
 "log"

 "github.com/Shopify/sarama"
)

func main() {
 // Конфигурация производителя
 config := sarama.NewConfig()
 config.Producer.Return.Successes = true
 config.Producer.Return.Errors = true

 producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
 if err != nil {
  log.Fatalln("Failed to create producer:", err)
 }
 defer producer.AsyncClose()

 // Отправка сообщений
 message := &sarama.ProducerMessage{
  Topic: "my-topic",
  Value: sarama.StringEncoder("Hello from Sarama!"),
 }
 producer.Input() <- message

 // Получение ответа
 select {
 case success := <-producer.Successes():
  fmt.Printf("Message is delivered: %sn", success.Offset)
 case err := <-producer.Errors():
  fmt.Printf("Failed to produce message: %sn", err.Err.Error())
 }

 // Конфигурация потребителя
 consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, sarama.NewConfig())
 if err != nil {
  log.Fatalln("Failed to create consumer:", err)
 }
 defer consumer.Close()

 // Получение сообщений
 partitionConsumer, err := consumer.ConsumePartition("my-topic", 0, sarama.OffsetNewest)
 if err != nil {
  log.Fatalln("Failed to start consumer:", err)
 }
 defer partitionConsumer.Close()

 for message := range partitionConsumer.Messages() {
  fmt.Printf("Message topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %sn",
   message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
 }
}

tasks 15 

from confluent_kafka import Producer, Consumer, avro
from confluent_kafka.schema_registry import SchemaRegistryClient

# Определение схемы Avro
schema = avro.loads(
    """
    {
      "type": "record",
      "name": "User",
      "fields": [
{"name": "name", "type": "string"},
{"name": "age", "type": "int"}
]
    }
)

# Создание клиента Schema Registry
schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})

# Создание производителя
producer = Producer({'bootstrap.servers': 'localhost:9092',
                   'schema.registry.url': 'http://localhost:8081'})

# Создание консьюмера
consumer = Consumer({'bootstrap.servers': 'localhost:9092',
                    'schema.registry.url': 'http://localhost:8081',
                    'group.id': 'my-group',
                    'auto.offset.reset': 'earliest'})

# Отправка сообщения
user = {'name': 'John', 'age': 30}
producer.produce('avro-topic', value=user, value_schema=schema)
producer.flush()

# Получение сообщения
consumer.subscribe(['avro-topic'])
message = consumer.poll(timeout=10)

if message is None:
    print("No message received")
else:
    # Десериализация сообщения
    user = avro.loads(message.value(), schema)
    print(f'User: {user}')

tasks 16 

from kafka import KafkaAdminClient

admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])

# Создание топика
admin_client.create_topics(
    [{'topic': 'new-topic', 'num_partitions': 3, 'replication_factor': 1}])

# Удаление топика
admin_client.delete_topics(['new-topic'])

tasks 17 


from kafka import KafkaConsumer
import threading

def consumer_thread(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])
    for message in consumer:
        print(f'Thread: {threading.current_thread().name}, Message: {message.value.decode("utf-8")}')

topics = ['multi-thread-topic']
threads = []
for topic in topics:
    thread = threading.Thread(target=consumer_thread, args=(topic,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

tasks 18

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamsExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("stream-topic");
        stream.mapValues(value -> "Processed: " + value)
              .to("processed-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

tasks 19 

{
  "name": "kafka-s3-sink",
  "connector.class": "io.confluent.connect.s3.S3SinkConnector",
  "tasks.max": "1",
  "topic": "my-topic",
  "s3.region": "us-east-1",
  "s3.bucket.name": "my-bucket",
  "s3.path.prefix": "data/",
  "s3.access.key": "your_access_key",
  "s3.secret.key": "your_secret_key",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter"
}

tasks 20 


from confluent_kafka import Producer, Consumer, avro
from confluent_kafka.schema_registry import SchemaRegistryClient

# Определение схемы Avro
schema = avro.loads(
    {
      "type": "record",
      "name": "User",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
  
    }
    """
)

# Создание клиента Schema Registry
schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})

# Создание производителя
producer = Producer({'bootstrap.servers': 'localhost:9092',
                   'schema.registry.url': 'http://localhost:8081'})

# Создание консьюмера
consumer = Consumer({'bootstrap.servers': 'localhost:9092',
                    'schema.registry.url': 'http://localhost:8081',
                    'group.id': 'my-group',
                    'auto.offset.reset': 'earliest'})

# Отправка сообщения
user = {'name': 'John', 'age': 30}
producer.produce('avro-topic', value=user, value_schema=schema)
producer.flush()

# Получение сообщения
consumer.subscribe(['avro-topic'])
message = consumer.poll(timeout=10)

if message is None:
    print("No message received")
else:
    # Десериализация сообщения
    user = avro.loads(message.value(), schema)
    print(f'User: {user}')
