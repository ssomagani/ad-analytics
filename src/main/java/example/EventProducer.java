/**
 * 
 */
package example;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class EventProducer {
	
	static final int MAX_RECEIVER_ID = 10000000;
	static final int MAX_CUSTOMER_ID = 10000;
	static final int MAX_LOCATION_ID = 100000;
	static HashMap<Integer, String> eventTypeMap = new HashMap<>();
	
	static {
		eventTypeMap.put(0, "recv_email");
		eventTypeMap.put(1, "open_email");
		eventTypeMap.put(2, "click_link");
		eventTypeMap.put(3, "buy_product");
	}
	
	static KafkaProducer<Long, String> producer;
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		producer = getProducer(args);
		
		EventsThread eventsThread = new EventsThread();
		VisitsThread visitsThread = new VisitsThread();
		
		eventsThread.run();
		visitsThread.run();
		
		producer.close();
	}
	
	private static class VisitsThread implements Runnable {
		@Override
		public void run() {
			final Random rand = ThreadLocalRandom.current();
			for(int i=0; i<Integer.MAX_VALUE; i++) {
				long receiver_id = rand.nextInt(MAX_RECEIVER_ID);
				int customer_id = rand.nextInt(MAX_CUSTOMER_ID);
				int location_id = rand.nextInt(MAX_LOCATION_ID);
				try {
					producer.send(new ProducerRecord<Long, String>(
							"visits", receiver_id, 
							receiver_id + "," + customer_id + location_id)).get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static class EventsThread implements Runnable {

		@Override
		public void run() {
			final Random rand = ThreadLocalRandom.current();
			for(int i=0; i<Integer.MAX_VALUE; i++) {
				long receiver_id = rand.nextInt(MAX_RECEIVER_ID);
				int customer_id = rand.nextInt(MAX_CUSTOMER_ID);
				
				try {
					producer.send(new ProducerRecord<Long, String>(
							"events", receiver_id, 
							receiver_id + "," + customer_id + eventTypeMap.get(rand.nextInt(4)))).get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static KafkaProducer<Long, String> getProducer(String[] args) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}
}
