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

// This Kakfa consumer is just a wrapper for the Event bus topic really.
// This version consumes Load and Unload events!

public class RFIDEventConsumer extends Thread {
	  private final KafkaConsumer<String, RFIDEvent> consumer;
	  
	  private final String topic;

	  public RFIDEventConsumer(String topic)
	  {
	   
	    this.topic = topic;
	    
	 	Properties props = new Properties();    
	 	props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
	    props.put(ConsumerConfig.GROUP_ID_CONFIG, "KongoRFIDEventConsumer");
	    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
	    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
	    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
	    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	    String s = RFIDEventSerializer.class.getName(); 
	    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, s);
	    
	    this.consumer = new KafkaConsumer<>(props);
	    
	    System.out.println("topic " + this.topic + " partitions=" + this.consumer.partitionsFor(this.topic).size());
	  }
	 
	  @Override
	  public void run()
	  {
	    try
	    {
	    		long threadId = Thread.currentThread().getId();
		    	consumer.subscribe(Collections.singletonList(this.topic));
		    	System.out.println(threadId + " ********* RFIDEventConsumer subscribed to topic " + this.topic);
		    			    	
		    	while (true)
		    {
		        ConsumerRecords<String, RFIDEvent> records = consumer.poll(Long.MAX_VALUE);
		        
		        for (ConsumerRecord<String, RFIDEvent> record : records)
		        {
	        	  		System.out.println(threadId + " ***** RFIDEventConsumer records = " + records.count());
	        	  		System.out.println(threadId + " ***** RFIDEventConsumer, Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
	        	  		RFIDEvent s = record.value();
	        	  		s.print();
	        	  		
	        	  		// depending on which event type received create Unload or load and send to correct topic
	        	  		EventBus topic;
	        	  		if (s.load) // load
	        	  		{
	        	  			System.out.println("RFIDEventConsumer.run.load");
	        	  			// TODO Check order!
	        	  			// public RFIDLoadEvent(long time, String goodsKey, String warehouseKey, String truckKey)
	        	  			RFIDLoadEvent e = new RFIDLoadEvent(s.time, s.goodsKey, s.warehouseKey, s.truckKey);
	        	  			topic = Simulate.rfidLoadTopic;
	        	  			topic.post(e);
	        	  		}
	        	  		else // unload
	        	  		{
	        	  			System.out.println("RFIDEventConsumer.run.unload");
	        	  			topic = Simulate.rfidUnloadTopic;
	        	  			// 	public RFIDUnloadEvent(long time, String goodsKey, String truckKey, String warehouseKey
	        	  			RFIDUnloadEvent e = new RFIDUnloadEvent(s.time, s.goodsKey, s.truckKey, s.warehouseKey);
		    				topic.post(e);
	        	  		}
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