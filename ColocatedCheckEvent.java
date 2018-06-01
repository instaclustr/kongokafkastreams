package com.instaclustr.kongokafkastreams.blog;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

// request check if goodsKey allowed on trucksKey given Goods already loaded
public class ColocatedCheckEvent
{
	long time; 
	String goodsKey;
	String truckKey;
	
	public ColocatedCheckEvent(long t, String goods, String truck)
	{
		this.time = t;
		this.goodsKey = goods;
		this.truckKey = truck;
	}
}
