package com.demo.samplekafka;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SampleConsumer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String topicName = "events" ;
		String groupName = "eventsTopicGroup";

		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092,localhost:9093");
		props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = null;
		
		
		try {
			consumer = new KafkaConsumer<>(props);
			consumer.subscribe(Collections.singletonList(topicName));
			System.out.println(" message is published in consumer");
			@SuppressWarnings("deprecation")
			ConsumerRecords<String, String> records=consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(" Received message is" + record.key() + " value is " + record.value());
			}

		} catch (Exception ex) {
			ex.printStackTrace();

		}
		consumer.close();
	}
}
