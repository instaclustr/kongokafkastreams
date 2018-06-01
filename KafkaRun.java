package com.instaclustr.kongokafkastreams.blog;

/*
 * Top level main program to run Kongo simulation
 * Paul Brebner, Version 5.3, 1 June 2018
 * run the main() here, and also run the main() in OverloadStreams.java to run the streams application to produce truck overload warnings.
 */

public class KafkaRun
{
	
	 public static void main(String[] args)
	 {
	     System.out.println("Welcome to the Instaclustr KONGO IoT Demo Application for Kafka!");
	     
	     if (Simulate.oneTopic)
	    	 	System.out.println("MODE = one topic with single consumer");
	     else
	    	 	System.out.println("MODE = multiple topics, oner per location, each Goods subscribed to location topic");
	     
	     if (Simulate.oneTopic)
	     {
	    	 	int numSensorConsumers = 1;
	    	 	System.out.println("Starting numSensorConsumers=" + numSensorConsumers + " subscribed to " + Simulate.kafkaSensorTopicBase);
	    	 	
	    	 	for (int i=0; i < numSensorConsumers; i++) {
	    	 		SensorConsumer sensorConsumer = new SensorConsumer(Simulate.kafkaSensorTopicBase);
	    	 		sensorConsumer.start();
	    	 	}
	     }
	     
	     if (Simulate.kafkaRFIDConsumerOn)
	     {
	    	 	// create RFID consumers here
	    	 	System.out.println("Started kafka RFIDEventConsumer on topic " + Simulate.rfidTopic);

	    	 	RFIDEventConsumer rfidConsumer = new RFIDEventConsumer(Simulate.rfidTopic);
	    	 	rfidConsumer.start();
	     }
	     
	     // Start simulation which produces event streams
	    	 Simulate simulateThread = new Simulate();
	    	 simulateThread.start();
	    	 
	    	 System.out.println("Kafka Run has started Kongo!");
	 }
}