package avro.com.jay.avro;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.jay.avro.Customer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class KafkaAvroConsumerV2 {

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("group.id", "my-avro-consumer-v2");
		properties.setProperty("enable.auto.commit", "false");
		properties.setProperty("auto.offset.reset", "earliest");
		
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserialize", KafkaAvroDeserializer.class.getName());
		properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
		properties.setProperty("specfic.avro.reader", "true");
		
		KafkaConsumer<String, Customer> consumer = new KafkaConsumer<String, Customer>(properties);
		String topic = "customer-avro";
		
		consumer.subscribe(Collections.singleton(topic));
		
		while (true) {
			ConsumerRecords<String, Customer> records = consumer.poll(500);
			for (ConsumerRecord<String, Customer> record : records) {
				Customer customer = record.value();
				System.out.println(customer);
			}
			consumer.commitSync();
		}
		
//		consumer.close();
	}
}
