package com.instaclustr.kongokafkastreams.blog;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

// at time LOAD or UNLOAD goodsKey at warehouseKey onto truckKey
public class RFIDEvent
{
	boolean load; // load = true, unload = false
	long time; 
	String goodsKey;
	String warehouseKey;
	String truckKey;
	
	public RFIDEvent(boolean t, long time, String goodsKey, String warehouseKey, String truckKey)
	{
		this.load = t;
		this.time = time;
		this.goodsKey = goodsKey;
		this.warehouseKey = warehouseKey;
		this.truckKey = truckKey;
	}
	
	public void print()
	{
		System.out.println(load + ", " + time + ", " + goodsKey + ", " + warehouseKey + ", " + truckKey);
	}
	
	public RFIDEvent()
	{
	}
	
	// can we put the handler in here? Why not! try and see....
	@Subscribe
	public void rfidEvent(RFIDEvent event)
	{
		if (event.load)
		{
		// at time LOAD goodsKey at warehouseKey onto truckKey
		System.out.println("RFID Load " + event);
		
		String locFrom = event.warehouseKey;
		String locTo = event.truckKey;
		
		// to move the goods need to get the Goods object itself
		// where is Goods now? claims to be at warehouseKey
		EventBus topicFrom = Simulate.topics.get(locFrom);
		
		// what if it's not?
		if (topicFrom == null)
			System.out.println("RFID LOAD EXCEPTION can't find topic for location " + locFrom);
		
		// This requires access to the global list of allGoods - nasty?!
		Goods goods = Simulate.allGoods.get(event.goodsKey);
		
		// unregister goods from warehouse topic location
		if (topicFrom != null) System.out.println("unregister from warehouse " + topicFrom.identifier());

		try
		{
			topicFrom.unregister(goods);
		}
		catch (Exception e)
		{
			// TODO Produce violation event
			System.out.println("LOAD EVENT Violation: Goods= " + event.goodsKey + " could not be loaded from warehouse location " + locFrom);
		}
		
		// change location
		// goods.locKey = locTo;
		EventBus topicTo = Simulate.topics.get(locTo);
		System.out.println("register with truck " + topicTo.identifier());
		
		// TODO Also check that truck is at same location as warehouse and produce violation if not, this is a business rule I guess.

		topicTo.register(goods);
			
		System.out.println("post co-location event to " + topicTo.identifier());
		
		ColocatedCheckEvent ce = new ColocatedCheckEvent(time, event.goodsKey, event.truckKey);
		
		// Possible BUG, Goods have already been moved to the truck so will receive the check event themselves, better to move after checking!?
		// BUt can't guarantee event order in distributed version? Put check in colocated check method to ignore self checking.
		
		// send this event to the location topics as all goods at the truck location will need to be check their rules.
		topicTo.post(ce);			
		}
		else 
		{
			// unload
			// at time UNLOAD goodsKey from truckKey to warehouseKey
			System.out.println("RFID UNLOAD Goods=" + event.goodsKey + " from truck " + event.truckKey + " to warehouse " + event.warehouseKey);
			
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
}
