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
 * (De)serializes RFIDUnloadEvent objects from/to strings.
 */

public class RFIDUnloadEventSerializer implements Closeable, AutoCloseable, Serializer<RFIDUnloadEvent>, Deserializer<RFIDUnloadEvent> {
	
    public static final Charset CHARSET = Charset.forName("UTF-8");

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }
    
    @Override
    public byte[] serialize(String s, RFIDUnloadEvent rfid) {
        // String line = String.format(Locale.ROOT, "%i,%s,%s,%.4f", sensor.time, sensor.doc, sensor.tag, sensor.metric, sensor.value);
    		String line =
    				rfid.time + ", " +
    				rfid.warehouseKey + ", " +
    				rfid.goodsKey + ", " +
    				rfid.truckKey;
        return line.getBytes(CHARSET);
    }
	
    @Override
    public RFIDUnloadEvent deserialize(String topic, byte[] bytes) {
        try {
            String[] parts = new String(bytes, CHARSET).split(", ");

            int i = 0;
            long time = Long.parseLong(parts[i++]);
            String warehouseKey = parts[i++];
            String goodsKey = parts[i++];
            String truckKey = parts[i++];
            
        // 	public RFIDUnloadEvent(long time, String goodsKey, String truckKey, String warehouseKey)

            return new RFIDUnloadEvent(time, goodsKey, truckKey, warehouseKey);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes", e);
        }
    }

    @Override
    public void close() {

    }
}