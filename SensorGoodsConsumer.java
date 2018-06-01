package com.instaclustr.kongokafkastreams.blog;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.WakeupException;

import com.google.common.eventbus.EventBus;

// public class SensorConsumer implements Runnable {
// Version of sensor consumer that is per Goods object 
// Just a consumer wrapper around Goods, could just put the code into Goods!

public class SensorGoodsConsumer extends Thread {
	  private final KafkaConsumer<String, Sensor> consumer;
	  
	  private final String topic;
	  private final Goods goods;
	  
	  public SensorGoodsConsumer(String topic, Goods g)
	  {
	   
	    this.topic = topic;
	    this.goods = g;
	    
	 	Properties props = new Properties();    
	 	props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
	 	// TODO Need different group for each Goods consumer? Just use topic name???  No, each Goods must be in a group by itself otherwise it won't get sensor event!
	 	// use goods tag, this is assuming each Goods can only be in one location at a time, correct.
	    props.put(ConsumerConfig.GROUP_ID_CONFIG, g.tag);
	    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
	    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
	    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
	    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	    String sensorSerializer = SensorSerializer.class.getName(); 
	    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, sensorSerializer);
	    
	    this.consumer = new KafkaConsumer<>(props);
	  }
	 
	  @Override
	  public void run()
	  {
	    try
	    {
		    	consumer.subscribe(Collections.singletonList(this.topic));
		    	System.out.println("********* SensorGoodsConsumer subscribed to topic " + this.topic + ", Goods = " + this.goods.tag);
		    			    	
		    	while (true)
		    {
		        ConsumerRecords<String, Sensor> records = consumer.poll(Long.MAX_VALUE);
		        
		        for (ConsumerRecord<String, Sensor> record : records)
		        {
	        	  		System.out.println("***** SensorGoodsConsumer records = " + records.count());
	        	  		System.out.println("***** SensorGoodsConsumer, Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
	        	  		Sensor s = record.value();
	        	  		s.print();
	        	  		goods.sensorEvent(s);
		        }
		    }   
	    }
	    catch (WakeupException e) {
	      // ignore for shutdown 
	    } finally {
	      consumer.close();
	    }
	  }

	  public void shutdown() {
	    consumer.wakeup();
	  }
	}