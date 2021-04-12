package msk;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {

	private static String TOPIC_NAME = "msk_test_topic";

	public static void main(String[] args) {
		Properties configs = new Properties();
		configs.put("bootstrap.servers",
				"b-3.mskworkshopcluste.9nyuen.c4.kafka.ap-northeast-2.amazonaws.com:9092,b-2.mskworkshopcluste.9nyuen.c4.kafka.ap-northeast-2.amazonaws.com:9092,b-1.mskworkshopcluste.9nyuen.c4.kafka.ap-northeast-2.amazonaws.com:9092");
		configs.put("group.id", "test_group");
		configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);

		consumer.subscribe(Arrays.asList(TOPIC_NAME));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(500);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record.value());
			}
		}
		
//		consumer.close();
	}
}
