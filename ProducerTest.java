package com.instaclustr.kongokafkastreams.blog;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


// generic producer for <String, Long> records, can be reused by using the send() method with topic as 1st argument.
public class ProducerTest {

	KafkaProducer<String, Long> producerSL;
	
	public ProducerTest() {	
		 Properties props; 
		 props = new Properties();        
		 props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		 producerSL = new KafkaProducer<>(props);
	}

	// method to send key=String value=Long to any topic
	public void send(String topic, String s, Long l)
	{
		ProducerRecord<String, Long> r = new ProducerRecord<String, Long>(topic, s, l);
		
		System.out.println("ProducerRecord = " + r.toString());
		
		try {	
			producerSL.send(r).get();
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		ProducerTest pt = new ProducerTest();
		pt.send("mytesttopic", "hello", 123L);		
	
	}

}
