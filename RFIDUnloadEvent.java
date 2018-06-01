package com.instaclustr.kongokafkastreams.blog;

// Note that we really don't need 2 different implementations for RFID events. Could just have one RFIDEvent class with both methods, and 
// two implementation classes for Load and Unload with no further stuff in them.

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

// at time UNLOAD goodsKey from truckKey to warehouseKey
public class RFIDUnloadEvent
{
	long time; 
	String warehouseKey;
	String goodsKey;
	String truckKey;
	
	public RFIDUnloadEvent(long time, String goodsKey, String truckKey, String warehouseKey)
	{
		this.time = time;
		this.goodsKey = goodsKey;
		this.warehouseKey = warehouseKey;
		this.truckKey = truckKey;
	}
	
	
	public RFIDUnloadEvent()
	{
	}
	
	public void print()
	{
		System.out.println(time + ", " + goodsKey + ", " + truckKey + ", " + warehouseKey);
	}
	
	// can we put the handler in here? Why not! try and see....
	// unload moves goods from truck to warehouse no checking done so just state change event
	// TODO Could check if the truck and warehouse have same environmental control!?
	@Subscribe
	public void rfidUnloadEvent(RFIDUnloadEvent event)
	{
		// at time UNLOAD goodsKey from truckKey to warehouseKey
		System.out.println("rfidUnloadEvent handler: UNLOAD Goods=" + event.goodsKey + " from truck " + event.truckKey + " to warehouse " + event.warehouseKey);
		
		String locFrom = event.truckKey;
		String locTo = event.warehouseKey;
		
		// to move the goods need to get the Goods object itself
		// where is Goods now? claims to be at truckKey
		EventBus topicFrom = Simulate.topics.get(locFrom);
		
		// This requires access to the global list of allGoods - nasty?!
		Goods goods = Simulate.allGoods.get(event.goodsKey);
		
		// unregister goods from truck topic location
		System.out.println("unregister from truck " + topicFrom.identifier());
		
		try 
		{
			topicFrom.unregister(goods);
		}
		catch (Exception e)
		{
			// TODO Produce violation event
			System.out.println("UNLOAD EVENT Violation: Goods= " + event.goodsKey + " could not be unloaded from truck location " + event.truckKey);
		}
		
		// change location
		// goods.locKey = locTo;
		EventBus topicTo = Simulate.topics.get(locTo);
		System.out.println("register with warehouse " + topicTo.identifier());
		topicTo.register(goods);
	}
}
