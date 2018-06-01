package com.instaclustr.kongokafkastreams.blog;

import java.io.Serializable;
import java.util.Map;

/*
 * Sensor objects are for warehouse and truck environmental metric values.
 */

public class Sensor implements Serializable, org.apache.kafka.common.serialization.Serializer {  

	long time;
	String doc;
	String tag;
	String metric;
	double value;
	
	public Sensor()
	{
		
	}

	public Sensor(long time, String doc, String tag, String metric, double value) {
		this.time = time;
		this.doc = doc;
		this.tag = tag;
		this.metric = metric;
		this.value = value;
	}
	
	public void print()
	{
		System.out.println(time + ", " + doc + ", " + tag + ", " + metric + "=" + value);
	}
	
	public String toStr()
	{
		return time + ", " + doc + ", " + tag + ", " + metric + "=" + value;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] serialize(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return null;
	}
}
