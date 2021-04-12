package msk;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;

public class Producer {

	private static String TOPIC_NAME = "msk_test_topic";
	private static KafkaProducer<String, String> producer;
	private static Random rand = new Random();

	public static void main(String[] args) {
		Properties configs = new Properties();
		configs.put("bootstrap.servers",
				"b-3.mskworkshopcluste.9nyuen.c4.kafka.ap-northeast-2.amazonaws.com:9092,b-2.mskworkshopcluste.9nyuen.c4.kafka.ap-northeast-2.amazonaws.com:9092,b-1.mskworkshopcluste.9nyuen.c4.kafka.ap-northeast-2.amazonaws.com:9092");
		configs.put("acks", "all");
		configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<String, String>(configs);
//		SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

		int maxCount = 5;
		if (null != args && args.length > 0) {
			try {
				maxCount = Integer.parseInt(args[0]);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if (maxCount < 0) {
			while (true) {
				sendMessage();
			}
		} else {
			for (int i = 0; i < maxCount; i++) {
				sendMessage();
			}
		}
		
		producer.flush();
		producer.close();
	}
	
	private static String[] avail_ticker = {"AAPL", "AMZN", "MSFT", "INTC", "TBV"};
	private static Random random = new java.util.Random();
	
	private static void sendMessage() {
		int random_ticker = random.nextInt(avail_ticker.length);
		
		JSONObject data = new JSONObject();
		data.put("EVENT_TIME", System.currentTimeMillis());
		data.put("TICKER", avail_ticker[random_ticker]);
		data.put("PRICE", rand.nextInt(10000));
		
		producer.send(new ProducerRecord<String, String>(TOPIC_NAME,
				data.toJSONString()));
//				String.format("%d|Hello MSK|%s", System.currentTimeMillis(), UUID.randomUUID())));
		try {
			Thread.sleep(rand.nextInt(100));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
//		System.out.println(String.format("%d|Hello MSK|%s", System.currentTimeMillis(), UUID.randomUUID()));
		System.out.println(data.toJSONString());
	}
}
