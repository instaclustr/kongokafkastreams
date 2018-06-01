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

public class RFIDUnloadEventConsumer extends Thread {
	  private final KafkaConsumer<String, RFIDUnloadEvent> consumer;
	  
	  private final String topic;

	  public RFIDUnloadEventConsumer(String topic)
	  {
	   
	    this.topic = topic;
	    
	 	Properties props = new Properties();    
	 	props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
	    props.put(ConsumerConfig.GROUP_ID_CONFIG, "KongoRFIDUnloadEventConsumer");
	    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
	    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
	    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
	    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	    String unloadSerializer = RFIDUnloadEventSerializer.class.getName(); 
	    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, unloadSerializer);
	    
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
		    	System.out.println(threadId + " ********* RFIDUnloadEventConsumer subscribed to topic " + this.topic);
		    			    	
		    	while (true)
		    {
		        ConsumerRecords<String, RFIDUnloadEvent> records = consumer.poll(Long.MAX_VALUE);
		        
		        for (ConsumerRecord<String, RFIDUnloadEvent> record : records)
		        {
	        	  		System.out.println(threadId + " ***** RFIDUnloadEventConsumer records = " + records.count());
	        	  		System.out.println(threadId + " ***** RFIDUnloadEventConsumer, Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
	        	  		RFIDUnloadEvent s = record.value();
	        	  		s.print();
	        	  		
	        	  		// there is only 1 topic for unload events
	    				EventBus topic = Simulate.rfidUnloadTopic;
	    				topic.post(s);
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