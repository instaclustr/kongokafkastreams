package com.instaclustr.kongokafkastreams.blog;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/*
 * New streams code for Kongo Blog 5.3.
 * Consumes records from 3 topics, dynamically computes truck weights, and produces warnings to another topic if trucks overloaded.
 * Streams code is run independently of the main simulation code, so run the main() here, as well as the main in KafkaRun.
 */
public class OverloadStreams {
	
	// trick to ensure clean start environment each time for testing/debugging, but need to create some topics manually.
  	final static String version = "8";     
  	// These 2 topics are now populated by the simulation code initially.
	final static String goodsWeightTopic = "kongo-goods-weight-" + version;
	final static String trucksMaxWeightTopic = "kongo-trucks-maxweight-" + version;
	
	final static String rfidTopic = "kongo-rfid-" + version;	// rfid load/unload events topic
	
	final static boolean printOn = true;		// print progress
	final static boolean cacheOn = true;		// KTable caching on/off (off is better for debugging)
	final static int cacheTime = 30000;		// if cache on, max time before records are sent.
	final static boolean transactional = true;	// exactly_once semantics, which also results in atomic operations.
	final static int runFor = 3;					// minutes to run for before shutting down.

    public static void main(final String[] args) throws Exception {
   
    		// Streams configuration properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kongo-stream-" + version);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Note: As default Serdes are <String, String> need to specify if different in DSL operations.
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // if cacheOn can change cache time to smaller time, this can still be overridden by exactly_once
        if (cacheOn) config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, cacheTime);
        
        // atomic operations (transactional)
        // prevent truck weights from going negative!
        // Note that this also changes commit_interval to 100ms, reducing the chance of getting stale data from KTable caches
        if (transactional) config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        
    		// Disable record cache for debugging
        if (!cacheOn) config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        
        // Create Streams SerDes from existing Serializer.
        final Serializer <RFIDEvent> rfidStreamSerializer = new RFIDEventSerializer();
        final Deserializer <RFIDEvent> rfidStreamDeserializer = new RFIDEventSerializer();
        final Serde <RFIDEvent> rfidStreamSerde = Serdes.serdeFrom(rfidStreamSerializer, rfidStreamDeserializer);
        
        StreamsBuilder builder = new StreamsBuilder();
       
        // Unified RFID event stream <String, RFIDEvent> Must specify the Serdes here as different to defaults.
        // This will be split into load/unload streams
        KStream<String, RFIDEvent> rfidStream =
        		builder
        		.stream(rfidTopic,
        				Consumed.with(
        						Serdes.String(), 	/* key serde */
        						rfidStreamSerde   	/* value serde */
        							) 
        		);
       
if (printOn) rfidStream.print();
      
// table: <goods, weight> <String, Long>
// must be initialized initially
KTable<String, Long> goodsWeightTable = builder.table(goodsWeightTopic, Consumed.with(Serdes.String(), Serdes.Long()));

if (printOn) goodsWeightTable.print();

// table: <truck, maxWeight> <String, Long>
// must be initialized initially
KTable<String, Long> trucksMaxWeightTable = builder.table(trucksMaxWeightTopic, Consumed.with(Serdes.String(), Serdes.Long()));

// table: <truck, weight> <String, Long>
// If entry doesn't exist it's created automatically.
KTable<String, Long> trucksWeightTable = builder.table("kongo-trucks-weight-" + version, Consumed.with(Serdes.String(), Serdes.Long()));

if (printOn) trucksWeightTable.print();

// (A) Split RFID event stream into load and unload events based on content of the event
// Two cases, one for load, one for unload
KStream<String, String> rfidLoadStream =
	// <key, rfidEvent> 
	rfidStream
	.filter((key, rfidEvent) -> (rfidEvent.load))
	.map((key, rfidEvent) ->  KeyValue.pair(rfidEvent.goodsKey, rfidEvent.truckKey));

// (B) unload events
KStream<String, String> rfidUnLoadStream =
	rfidStream
	.filter((key, rfidEvent) -> (!rfidEvent.load))
	.map((key, rfidEvent) ->  KeyValue.pair(rfidEvent.goodsKey, rfidEvent.truckKey));

if (printOn) rfidLoadStream.print();
if (printOn) rfidUnLoadStream.print();

/*
// Alternative code for (A) (B) using a branch.
// can't do a map at end of this as branch is terminal, so have to return as RFIDEvent
KStream<String, RFIDEvent>[] rfidbranches =
	rfidStream
	.branch(
			(key, rfidEvent) -> rfidEvent.load,
			(key, rfidEvent) -> !rfidEvent.load
			);

KStream<String, String> rfidLoadStream = 
	rfidbranches[0]
	.map(
			(key, rfidEvent) ->  KeyValue.pair(rfidEvent.goodsKey, rfidEvent.truckKey)
		);

KStream<String, String> rfidUnLoadStream = 
	rfidbranches[1]
	.map(
		(key, rfidEvent) ->  KeyValue.pair(rfidEvent.goodsKey, rfidEvent.truckKey)
	);

*/

// stream with updated (increased) truck weight resulting from load events
KStream<String, Long> trucksWeightStream =
	// <goods, truck>
	rfidLoadStream
	// <goods, truck> join <goods, weight> -> <goods, (truck, weight)>
	.join(goodsWeightTable,
			(truck, weight) -> new ReturnObjectSL(truck, weight),
			Joined.with(
						Serdes.String(), 	// key
						Serdes.String(),   	// left value
						Serdes.Long())  		// right value
	)
	// <goods, (truck, weight) -> <truck, weight>
	.map((goods, returnObject) ->  KeyValue.pair(returnObject.s, returnObject.v))
	// <truck, weight> leftJoin <truck, oldWeight> -> <truck, weight+oldWeight>
	.leftJoin(trucksWeightTable,
		(goodsWeight, oldWeight) -> oldWeight == null ? goodsWeight : goodsWeight + oldWeight,
		Joined.with(
				Serdes.String(), 	// key
				Serdes.Long(),   	// left value
				Serdes.Long())  		// right value	
	); 

// update state of trucks weight KTable
trucksWeightStream.to("kongo-trucks-weight-" + version, Produced.with(Serdes.String(), Serdes.Long()));

// check new truck weight against truck maxweight and produce violation warning
KStream<String, String> overloadWarningStream =
	// <truck, weight>
	trucksWeightStream
	// <truck, weight> join <truck, maxWeight>
	.join(trucksMaxWeightTable,
			(weight, max) -> weight > max ? "overloaded! " + weight + " > " + max : "",
					Joined.with(
							Serdes.String(), 	// key
							Serdes.Long(),   	// left value
							Serdes.Long())  		// right value	
					
	)
	.filter((key, value) -> (value.length() >= 1));

overloadWarningStream.to("kongo-overload-warnings");
if (printOn) overloadWarningStream.print();

// stream with updated (decreased) truck weight resulting from unload events
KStream<String, Long> trucksWeightStream2 =
	// <goods, truck>
	rfidUnLoadStream
	// <goods, truck> join <goods, weight> -> <goods, (truck, weight)>
	.join(goodsWeightTable,
			(truck, weight) -> new ReturnObjectSL(truck, weight),
			Joined.with(
						Serdes.String(), 	// key
						Serdes.String(),   	// left value
						Serdes.Long())  		// right value
	)
	// <goods, (truck, weight) -> <truck, weight>
	.map((goods, returnObject) ->  KeyValue.pair(returnObject.s, returnObject.v))
	// <truck, weight> leftJoin <truck, oldWeight> -> <truck, oldWeight-weight>
	.leftJoin(trucksWeightTable,
		(goodsWeight, oldWeight) -> oldWeight == null ? 0 : oldWeight - goodsWeight,			
		Joined.with(
				Serdes.String(), 	// key
				Serdes.Long(),   	// left value
				Serdes.Long())  		// right value	
	); 

// update state of trucks weight KTable
trucksWeightStream2.to("kongo-trucks-weight-" + version, Produced.with(Serdes.String(), Serdes.Long()));

if (printOn) trucksWeightStream.print();
if (printOn) trucksWeightStream2.print();

  		final Topology top = builder.build();  		
        KafkaStreams streams = new KafkaStreams(builder.build(), config);   
        System.out.println(top.describe());
        
        // cleanUp() actually recreates the state from the topic! I.e. doesn't reset it!!!
        streams.cleanUp();
        streams.start();
        long t = runFor * 60 * 1000;
        System.out.println("Stopping in..." + t);
	    Thread.sleep(t);
	    System.out.println("Closing up shop!");
	    streams.close();	  
    }
}