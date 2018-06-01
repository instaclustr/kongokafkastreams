package com.instaclustr.kongokafkastreams.blog;

import java.io.Serializable;
import java.util.Map;

/*
 * Violation events for sensor and truck location violation messages.
 * TODO Does this need a serializer????
 */

public class ViolationEvent implements Serializable, org.apache.kafka.common.serialization.Serializer {  

	long time;
	String doc;			// violation type
	String goodsTag;		// which Goods had the problem?
	String goodsCats;	// String goods categories
	String location; 	// problem in warehouse or truck location
	String violation;	// what were the problems?
	
	public ViolationEvent()
	{
		
	}

	public ViolationEvent(long time, String doc, String goodsTag, String goodsCats, String location, String violation) {
		this.time = time;
		this.doc = doc;
		this.goodsTag = goodsTag;
		this.goodsCats = goodsCats;
		this.location = location;
		this.violation = violation;
	}
	
	public void print()
	{
		System.out.println(time + ", " + doc + ", " + goodsTag + ", " + goodsCats + ", " + location + ", " + violation);
	}
	
	public String toStr()
	{
		return time + ", " + doc + ", " + goodsTag + ", " + goodsCats + ", " + location + ", " + violation;
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
