package com.demo.samplekafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class SampleProducer {

	//private static final String[] topicName = { "TP.DEMANDIMPACT.INBOUND.EVENTS.BR.DEV"};
	private static final String[] topicName = { "events"};
	private static String eventkey = "6252705";
	private static String eventFeed = "{\"id\":6252705,\"type\":\"UPDATE_STATUS\",\"resource\":\"http://localhost:8080/exposed/event/6252705\",\"brand\":\"2\",\"market\":\"US\",\"channel\":\"RTL\",\"economicRegion\":\"US\",\"before\":{\"startDate\":\"2022-05-26T00:00:00Z\",\"endDate\":\"2022-05-26T00:00:00Z\",\"description\":\"KafkaTest123\",\"status\":\"PLANNED\",\"eventSeason\":\"PRESEASON\",\"locationDetails\":[{\"id\":\"000\",\"type\":\"All\",\"description\":\"ALL\",\"categoryId\":\"CHAIN\",\"currency\":\"USD\",\"promoPercentDiscountVal\":0.1,\"promoPricePointDiscountVal\":223.2,\"promoDollarOffDiscountVal\":24.8,\"discountDetails\":[{\"quantity\":1,\"tier\":\"FIRST\",\"discountType\":\"PERCENTAGE_OFF\",\"discountValue\":0.1}],\"promoted\":true}],\"merchandiseDetails\":[{\"merchandiseHierarchyId\":\"ff308bec-9085-49fe-8168-4ed800c6e8dc\",\"type\":\"PROGRAM\",\"id\":\"215383\",\"description\":\"SLIM RAPID MOVEMEN\",\"divisionId\":\"11\",\"divisionDescription\":\"MENS\",\"departmentId\":\"113\",\"departmentDescription\":\"MENS SUITING\",\"classId\":\"01\",\"classDescription\":\"SUIT BLAZERS\",\"subClassId\":\"01\",\"subClassDescription\":\"SUIT JACKETS\",\"minTicketPrice\":248.0,\"maxTicketPrice\":249.0}]},\"current\":{\"startDate\":\"2022-05-26T00:00:00Z\",\"endDate\":\"2022-05-26T00:00:00Z\",\"description\":\"KafkaTest456\",\"status\":\"DRAFT\",\"eventSeason\":\"PRESEASON\",\"locationDetails\":[{\"id\":\"000\",\"type\":\"All\",\"description\":\"ALL\",\"categoryId\":\"CHAIN\",\"currency\":\"USD\",\"promoPercentDiscountVal\":0.1,\"promoPricePointDiscountVal\":223.2,\"promoDollarOffDiscountVal\":24.8,\"discountDetails\":[{\"quantity\":1,\"tier\":\"FIRST\",\"discountType\":\"PERCENTAGE_OFF\",\"discountValue\":0.1}],\"promoted\":true}],\"merchandiseDetails\":[{\"merchandiseHierarchyId\":\"ff308bec-9085-49fe-8168-4ed800c6e8dc\",\"type\":\"PROGRAM\",\"id\":\"215383\",\"description\":\"SLIM RAPID MOVEMEN\",\"divisionId\":\"11\",\"divisionDescription\":\"MENS\",\"departmentId\":\"113\",\"departmentDescription\":\"MENS SUITING\",\"classId\":\"01\",\"classDescription\":\"SUIT BLAZERS\",\"subClassId\":\"01\",\"subClassDescription\":\"SUIT JACKETS\",\"minTicketPrice\":248.0,\"maxTicketPrice\":249.0}]},\"eventType\":\"PROMOTION\",\"lastModifiedTime\":\"2019-05-16T22:01:27.576Z\",\"routingKey\":\"pem.eventfeed.routekey.BR.US.RTL.PLANNED_ITEM_EVENT.DRAFT.UPDATE_STATUS\",\"source\":\"PEM\"}";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093");
		
		//props.put("bootstrap.servers", "wn0-devdik.waqq0sav5qee5crkuqcifcnjtf.dx.internal.cloudapp.net:9092, wn1-devdik.waqq0sav5qee5crkuqcifcnjtf.dx.internal.cloudapp.net:9092,wn2-devdik.waqq0sav5qee5crkuqcifcnjtf.dx.internal.cloudapp.net:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		
		Producer<String, String> producer = new KafkaProducer<>(props);

		for (int i = 0; i < topicName.length; i++) {

			try {
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName[i], eventkey, eventFeed);
				RecordMetadata ProducerResponse = producer.send(record).get();
				System.out.println(" Feed is published to topic :::" + record.topic()+ "  key is " + record.key()
				 + " event feed is " + record.value());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		producer.flush();
		producer.close();
	}
}