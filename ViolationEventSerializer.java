package com.instaclustr.kongokafkastreams.blog;

// https://github.com/nielsutrecht/kafka-serializer-example/blob/master/src/main/java/com/nibado/example/kafka/serialization/StringReadingSerializer.java
// TODO Wouldn't this work for any class that produces a string? Only for serializer, as deseriazer needs to know fields.

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;

/**
 * (De)serializes ViolationEvent objects from/to strings.
 */

public class ViolationEventSerializer implements Closeable, AutoCloseable, Serializer<ViolationEvent>, Deserializer<ViolationEvent> {
	
    public static final Charset CHARSET = Charset.forName("UTF-8");

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }
    
    //@Override
    public byte[] oldserialize(String s, ViolationEvent violationEvent) {
    	
    		String line =
    				violationEvent.time + ", " +
    				violationEvent.doc + ", " +
    				violationEvent.goodsTag + ", " +
    				violationEvent.goodsCats + ", " + 
    				violationEvent.location + ", " + 
    				violationEvent.violation;		
        return line.getBytes(CHARSET);
    }
    
    // JSON version!
    // want output like this, what about leading/trailing double quotes?!:   {"goods": "goods1", "error": "blah blah"}
    // add type field = "sensor" or "location" which is just doc variable.
    @Override
    public byte[] serialize(String s, ViolationEvent violationEvent) {
    	
    		String goods = violationEvent.goodsTag; 
    		String error = violationEvent.violation;
    		// String type = violationEvent.doc;

    		String line =
    				"{\"goods\": \"" +
    				goods +
    				"\", " + 
    				//"\"type\": \"" +
    				//type +
    				//"\", " + 
    				"\"error\": \"" +
    				error +
    				"\"}";

    		String lineOld =
    				violationEvent.time + ", " +
    				violationEvent.doc + ", " +
    				violationEvent.goodsTag + ", " +
    				violationEvent.goodsCats + ", " + 
    				violationEvent.location + ", " + 
    				violationEvent.violation;		

        return line.getBytes(CHARSET);
    }
	
    // deserialize not used as events consumed by Kafka connector.
    @Override
    public ViolationEvent deserialize(String topic, byte[] bytes) {
        try {
            String[] parts = new String(bytes, CHARSET).split(", ");

            int i = 0;
            long time = Long.parseLong(parts[i++]);
            String doc = parts[i++];
            String goodsTag = parts[i++];
            String goodsCats = parts[i++];
            String location = parts[i++];
            String violation = parts[i++];

            return new ViolationEvent(time, doc, goodsTag, goodsCats, location, violation);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes", e);
        }
    }

    @Override
    public void close() {

    }
}