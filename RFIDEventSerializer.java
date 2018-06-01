package com.instaclustr.kongokafkastreams.blog;

// https://github.com/nielsutrecht/kafka-serializer-example/blob/master/src/main/java/com/nibado/example/kafka/serialization/StringReadingSerializer.java
// TODO Wouldn't this work for any class that produces a string? Only for serializer, as deseriazer needs to know fields.
// Generic version for multiple event types per topic

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;

/**
 * (De)serializes RFIDLoadEvent objects from/to strings.
 */

public class RFIDEventSerializer implements Closeable, AutoCloseable, Serializer<RFIDEvent>, Deserializer<RFIDEvent> {
	
    public static final Charset CHARSET = Charset.forName("UTF-8");

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }
    
    // public RFIDEvent(boolean t, long time, String goodsKey, String warehouseKey, String truckKey)
    
    @Override
    public byte[] serialize(String s, RFIDEvent rfid) {
    		String line =
    				rfid.load + ", " +
    				rfid.time + ", " +
    				rfid.goodsKey + ", " +
    				rfid.warehouseKey + ", " +
    				rfid.truckKey;
        return line.getBytes(CHARSET);
    }
	
    @Override
    public RFIDEvent deserialize(String topic, byte[] bytes) {
        try {
            String[] parts = new String(bytes, CHARSET).split(", ");

            int i = 0;
            
            boolean load = Boolean.parseBoolean(parts[i++]);
            long time = Long.parseLong(parts[i++]);
            String goodsKey = parts[i++];
            String warehouseKey = parts[i++];
            String truckKey = parts[i++];
            
            // public RFIDEvent(boolean t, long time, String goodsKey, String warehouseKey, String truckKey)

            return new RFIDEvent(load, time, goodsKey, warehouseKey, truckKey);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes", e);
        }
    }

    @Override
    public void close() {

    }
}