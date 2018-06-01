/*
 * Goods are things that are stored in Warehouses or transported in Trucks.
 * Every Good is created with a random set of categories so no two goods are the same.
 * Depending on categories, Goods have rules about which goods can be transported together or ranges of sensor values during storage or transport which can be violated.
 * New version which checks if Goods being loaded are damaged by already loaded Goods, and if any already Goods are damaged by newly loaded Goods.
 * Note that some of these violations could be treated as business process related rather than strict violations.
 * E.g. edible and medicinal doesn't immediately break the medicinal, but rather the business rules for storage of medicinal goods due to rodents.
 * 
 */


package com.instaclustr.kongokafkastreams.blog;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.eventbus.Subscribe;

import java.util.*;

public class Goods {

	final String prefix = "goods_"; // what am i?
	
       String tag;	// each Goods has a unique random unique RFID tag
       
       static KafkaProducer<String, ViolationEvent> producerViolationEvents = null;	// new kafka producer for violation events TODO Do we need to set properties? Seems to work ok without any.
       
       // simple approach to categories to allow Goods with multiple categories, add new Boolean var for each new category
       Boolean categoryPerishable = false;	// has a useByDate
       Boolean categoryHazardous = false; 	// Is Dangerous to other things. Single Hazardous type for time being, add sub-types later
       Boolean categoryFragile = false;		// Will break if vibrated or accelerated too much
       Boolean categoryEdible = false;		// Is food
       Boolean categoryMedicinal = false;	// Medicines or for medical use
       Boolean categoryDry = false;			// Keep dry, or may explode, rust, spoil, etc.
       Boolean categoryBulky = false; 		// heavy and big, may break other things
       // Environmental/temperature category, only 1 allowed
       Boolean categoryFrozenTemp = false;
       Boolean categoryHeatSensitiveTemp = false;
       Boolean categoryCoolTemp = false;
       Boolean categoryRoomTemp = false;
       Boolean categoryAmbientTemp = false;
       
       Temp temp = null; 			//  temperature object with actual ranges and comparison methods
       long useByDate;				// optional "use by date" for Perishable goods.
       String categories;			// string representing all true categories
       // a few properties such as weight etc
       Double weight;				// total weight in Kg of goods in package
       Long quantity;				// how many of same type of thing are associated with the tag
       // these properties are used to determine if the item is Bulky
       long height;					// dimensions in cm
       long width;
       long breadth;
       long volume;					// computed from dimensions

       // sensible min/max values
       final long maxWeight             = 10000;
       final long maxHeight             = 1000;
       final long maxWidth              = 1000;
       final long maxBreadth            = 1000;
       final Double minWeight           = 1.0;
       final long minHeight             = 1;
       final long minWidth              = 1;
       final long minBreadth            = 1;
       final long minQuantity           = 1;
       final long maxQuantity           = 10000;

       // create a random Goods
       public Goods()
       {

	    	   	// create producer for violation events if not created yet.
	    	   	if (producerViolationEvents == null)
	    	   	{  
	    	   		Properties props;
	    	   		props = new Properties();        
	    	   		props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
	    	   		props.put("client.id", "KongoViolationEvent");
	    	   		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");	    
	    	   		String violationEventSerializer = ViolationEventSerializer.class.getName(); 
	    	   		props.put("value.serializer", violationEventSerializer);
	    	   		producerViolationEvents = new KafkaProducer<>(props);
	    	   		System.out.println("Kong publishing violation events to topic " + Simulate.violationTopic);
	    	   	}
	    	   
              Random rand = new Random();

              // associate tag with this Goods
              this.tag = this.prefix + randomUUID();
              double lambda = 0.01;
              this.weight = Math.log(1-rand.nextDouble())/(-lambda);
              this.quantity = (long) rand.nextInt((int) (this.maxQuantity-this.minQuantity)) + this.minQuantity;
           
              lambda = 0.001;
              this.height =  (long) (Math.log(1-rand.nextDouble())/(-lambda));
              this.width = (long) (Math.log(1-rand.nextDouble())/(-lambda));
              this.breadth = (long) (Math.log(1-rand.nextDouble())/(-lambda));
              this.volume = this.height * this.width * this.breadth;
              
              this.useByDate = Long.MAX_VALUE;
              this.categoryPerishable = rand.nextDouble() < 0.2;
              if (this.categoryPerishable)
            	  	this.useByDate = rand.nextInt(100000);
              
              this.categoryHazardous = rand.nextDouble() < 0.1;
              this.categoryFragile = rand.nextDouble() < 0.05;
              this.categoryEdible = rand.nextDouble() < 0.2;
              this.categoryMedicinal = rand.nextDouble() < 0.2;
              
              // bulky if (any dimension > 1m || volume > (50*50*50)) and weight > 100kg
              this.categoryBulky = (this.height > 100.0 || this.width > 100.0 || this.breadth > 100.0 || this.volume > 125000) && this.weight >= 100.0;
              
              // decide if it has a temperature category and pick only one.
              // 1.0 chance if perishable or medicinal
              if (rand.nextDouble() < 0.2 || (this.categoryMedicinal || this.categoryPerishable))
              {
            	  	int r = rand.nextInt(5);
            	  	switch (r)
            	  	{
            	  		case 0: this.categoryFrozenTemp = true; temp = new Temp(0); break;
            	  		case 1:	this.categoryHeatSensitiveTemp = true; temp = new Temp(1); break;
            	  		case 2:	this.categoryCoolTemp = true; temp = new Temp(2); break;
            	  		case 3:	this.categoryRoomTemp = true; temp = new Temp(3); break;
            	  		case 4:	this.categoryAmbientTemp = true; temp = new Temp(4); break;
            	  	}
              }
             
              this.categoryDry = rand.nextDouble() < 0.2;
              // create string representation of all categories
              this.categories = allCategories();
       }

       // return random UUID as String
       public static String randomUUID()
       {
    	   		UUID uuid = UUID.randomUUID();
    	   		String r = uuid.toString();
    	   		return r;
       }

       
       // return all categories as a String
       public  String allCategories()
       {
    	   		String s = "";
    	   	
    	   		if (categoryPerishable) s += "P";
    	   		if (categoryHazardous) s += "H";
    	   		if (categoryFragile) s+= "F";  
    	   		if (categoryEdible) s += "E";
    	   		if (categoryMedicinal) s += "M";
    	   		if (categoryBulky) s += "B";
    	   		if (categoryDry) s += "D";
    	   		if (categoryFrozenTemp) s += "T1";
    	   		if (categoryHeatSensitiveTemp) s += "T2";
    	   		if (categoryCoolTemp) s += "T3";
    	   		if (categoryRoomTemp) s += "T4";
    	   		if (categoryAmbientTemp) s += "T5";
	  	
    	   		return s;
       }
       
       // New Event Bus method to subscribe to sensor events from sensor topics
       @Subscribe
       public void sensorEvent(Sensor sensor)
       {
			// System.out.println("GOT SENSOR EVENT! Object=" +  tag + ", event=" + sensor.toStr());
			
			// got a sensor event for this goods at the warehouse it is in. What to do with it? Check rules!
			String v = violatedSensorCatRules(sensor);
			
			if (!v.equals(""))
			{
				// Add sensor data: Problem is that this isn't sufficient, e.g. for use by date need to the current time
				String sensorViolation = sensor.metric + "=" + sensor.value + ": " + v;
				
				
				if (Simulate.kafkaViolationTopicOn)
				{
					// write violations to Kafka topic for connector to pick up and write to Cassandra
					// create violation event object: fields are Goods id, Goods categories (String), location tag (warehouse id), violations (String)
					// what about location violations? location is truck, but may need the tag and categories of the objecting Goods already in the truck?  Or is that in the violation String already?
					// public ViolationEvent(long time, String doc, String goodsTag, String goodsCats, String location, String violation) {
										
					ViolationEvent ve = new ViolationEvent(sensor.time, "sensor", tag, allCategories(), sensor.tag, sensorViolation);
					
					ProducerRecord r = new ProducerRecord(Simulate.violationTopic, tag, ve);
					
					System.out.println("SENSOR RULE VIOLATION EVENT + " + ve.toStr());
					
					try {
						producerViolationEvents.send(r).get();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
				else System.out.println("SENSOR RULE VIOLATION for goods=" + tag + ", categories=" + allCategories() + " in warehouse " + sensor.tag + " violations: " + sensorViolation);
			}
       }
       
       @Subscribe
       public void colocatedRulesEvent(ColocatedCheckEvent event)
       {
			System.out.println("GOT Colocated Check EVENT! Object=" +  tag);
			
			if (tag != event.goodsKey)
			{
		
				// get the Goods object to check against
				Goods loadMe = Simulate.allGoods.get(event.goodsKey);
				if (loadMe != null)
				{
					
					// boolean happy = allowedInTruck(loadMe); // old generic version 
					boolean violation = loadedGoodsDamaged(loadMe);
					
					// Goods being loaded has violation due to goods already Loaded
					if (violation)
					{
						
						String violations = notAllowedInTruckViolations(loadMe);
						String violationString = allCategories() + ": " + violations;
						
						System.out.println("DEBUG: goods loaded violation " + violationString);
									
						if (Simulate.kafkaViolationTopicOn)
						{
							
							// write violations to Kafka topic for connector to pick up and write to Cassandra
							// create violation event object: fields are Goods id, Goods categories (String), location tag (warehouse id), violations (String)
							// what about location violations? location is truck, but may need the tag and categories of the objecting Goods already in the truck?  Or is that in the violation String already?
							// public ViolationEvent(long time, String doc, String goodsTag, String goodsCats, String location, String violation) {
							ViolationEvent ve = new ViolationEvent(event.time, "location", event.goodsKey, loadMe.allCategories(), this.tag, violationString);
							
							ProducerRecord r = new ProducerRecord(Simulate.violationTopic, event.goodsKey, ve);
							
							
							System.out.println("Goods being loaded VIOLATION EVENT! " + ve.toStr());
							
							try {
								producerViolationEvents.send(r).get();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (ExecutionException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
						}
						else System.out.println("Goods already on truck " + tag + " damaged Goods being loaded " + event.goodsKey + " " + violationString);
						//event.violated = true;
					}
					
					// Now check if this Goods (which is already on the truck) has a violation due to the Goods being loaded (loadMe)
					violation = alreadyLoadedGoodsDamaged(loadMe);
					
					if (violation)
					{
						String violations = notAllowedInTruckViolations(loadMe);
						String violationString = loadMe.allCategories() + ": " + violations;
						
						if (Simulate.kafkaViolationTopicOn)
						{
							// write violations to Kafka topic for connector to pick up and write to Cassandra
							// create violation event object: fields are Goods id, Goods categories (String), location tag (warehouse id), violations (String)
							// what about location violations? location is truck, but may need the tag and categories of the objecting Goods already in the truck?  Or is that in the violation String already?
							// public ViolationEvent(long time, String doc, String goodsTag, String goodsCats, String location, String violation) {
							ViolationEvent ve = new ViolationEvent(event.time, "location", this.tag, this.allCategories(), event.goodsKey, violationString);
							
							ProducerRecord r = new ProducerRecord(Simulate.violationTopic, this.tag, ve);
							
							System.out.println("Goods already loaded VIOLATION EVENT! " + ve.toStr());

							try {
								producerViolationEvents.send(r).get();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (ExecutionException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						else System.out.println("Goods on truck " + tag + " damaged by Goods being loaded = " + event.goodsKey + " " + violationString);
						//event.violated = true;
					}
				}
			}
			//}
			//else System.out.println("Colocated event check: already violated so ignoring!");
       }
       
       // methods to check if 2 Goods are allowed to be transported in the same truck based on categories
       // simplified compared with original intention, as no hazardous goods are allowed together.
       public boolean notAllowedInTruck(Goods y)
       {
    	   		if (		categoryFragile && y.categoryBulky ||
    	   				categoryBulky && y.categoryFragile ||
    	   				categoryHazardous && (y.categoryEdible || y.categoryMedicinal) ||
    	   				(categoryEdible || categoryMedicinal) && y.categoryHazardous ||
    	   				categoryEdible && y.categoryMedicinal ||
    	   				categoryMedicinal && y.categoryEdible ||
    	   				categoryHazardous && y.categoryHazardous
    	   		)
    	   			return true;
    	   		else return false;
       }
       
       // one directional version Goods passed as arg is the Goods being loaded onto the truck, so needs to be the one with the rule violation
       // tricky, what do we mean by not allowed on? Will it do damage to Goods on Truck? Will Goods on truck damage it?
       // do both
       public boolean loadedGoodsDamaged(Goods y)
       {
    	   // rules are: don't allow fragile goods on with bulky as they may get broken, edible or medicinal with Hazardous, medicinal with edible (as food attracts vermin), different types of hazardous
    	   		if (		
    	   				categoryBulky && y.categoryFragile ||
    	   				categoryHazardous && (y.categoryEdible || y.categoryMedicinal) ||
    	   				categoryEdible && y.categoryMedicinal ||
    	   				categoryHazardous && y.categoryHazardous
    	   		)
    	   			return true;
    	   		else return false;
       }
       
       // this version checks if the already loaded Goods (this) is damaged by the loaded goods (y)
       public boolean alreadyLoadedGoodsDamaged(Goods y)
       {
    	   // rules are: don't allow bulky goods on with fragile as they may get broken, Hazardous with edible or medicinal, edible with medicinal (as food attracts vermin), different types of hazardous
    	   if (		categoryFragile && y.categoryBulky ||
	   				(categoryEdible || categoryMedicinal) && y.categoryHazardous ||
	   				categoryMedicinal && y.categoryEdible ||
	   				categoryHazardous && y.categoryHazardous
	   		)
	   			return true;
	   		else return false;
       }
       
       public boolean allowedInTruck(Goods y)
       {
    	   		return !notAllowedInTruck(y);
       }
       
       // new version with String violation results, returns String null or error messages.
       public String notAllowedInTruckViolations(Goods y)
       {
    	   		String s = "";
    	   		
    	   		if (	categoryFragile && y.categoryBulky || categoryBulky && y.categoryFragile)
    	   			s += "Fragile AND Bulky; ";	
    	   		if (categoryHazardous && (y.categoryEdible || y.categoryMedicinal) ||
    	   				(categoryEdible || categoryMedicinal) && y.categoryHazardous)
    	   			s += "Hazardous AND (Edible OR Medicinal); ";
    	   				
    	   				
    	   		if (	categoryEdible && y.categoryMedicinal ||
    	   				categoryMedicinal && y.categoryEdible)
    	   			s += "Edible AND Medicinal;";
    	   		
    	   		if (	categoryHazardous && y.categoryHazardous)
    	   			s += "Hazardous AND Hazardous";
    	   		return s;
       }
       
             
       // sensor range checking methods. Only return false (violation) if the sensor metric value is present AND the value is out of range.
       // dry if not wet.
       public boolean dry(Sensor sensor)
       {
    	   		return !(sensor.metric.equals("humidity") && sensor.value > 60);
       }
       
       // dark if not light
       public boolean dark(Sensor sensor)
       {
    	   	return !(sensor.metric.equals("illuminace") && sensor.value > 500);
       }
       
       // return true if use by date is in the future
       public boolean useByDate(Sensor sensor)
       {
    	   		long now = sensor.time;
    	   		return (useByDate > now);
       }
       
       // temperature check methods which uses the Temp object
       public boolean tempCheck(Sensor sensor)
       {
    	   		return	!(sensor.metric.equals("temp") &&  !temp.tempInRange(sensor.value));
       }
       
       public boolean frozen(Sensor sensor)
       {
    	   		return	!(sensor.metric.equals("temp") && sensor.value > -20);
       }
       
       public boolean heatSensitive(Sensor sensor)
       {
    	   		return !(sensor.metric.equals("temp") && (sensor.value < 2 || sensor.value > 8));
       }
       
       public boolean cool(Sensor sensor)
       {
    	   		return !(sensor.metric.equals("temp") && (sensor.value < 8 || sensor.value > 15));
       }
       
       public boolean roomTemp(Sensor sensor)
       {
    	   		return !(sensor.metric.equals("temp") && (sensor.value < 15 || sensor.value > 25));
       }
       
       public boolean ambientTemp(Sensor sensor)
       {
    	   		return !(sensor.metric.equals("temp") && (sensor.value < 1 || sensor.value > 30));
       }
       
       // composite rules for categories
       public boolean rulePerishable(Sensor sensor)
       {
    	  		return dry(sensor) && dark(sensor) && useByDate(sensor);
       }
      
       public boolean ruleEdible(Sensor sensor)
       {
    	  		return dry(sensor);
       }
      
       public boolean ruleMedicinal(Sensor sensor)
       {
    	  		return dry(sensor) && dark(sensor);
       }
      
       public boolean lowG(Sensor sensor)
       {
    	   		return !(sensor.metric.equals("acceleration") && (sensor.value >= 5));
       }
      
       public boolean smoothRide(Sensor sensor)
       {
    	  		return !(sensor.metric.equals("vibrationDisplacement") && sensor.value >= 10 ||
    	  				sensor.metric.equals("vibrationVelocity") && sensor.value >= 100);
       }
      
      public boolean ruleFragile(Sensor sensor)
      {
    	  		return lowG(sensor) && smoothRide(sensor);
      }
      
      // given the categories check against the sensor metric/values
      public boolean checkSensorCatRules(Sensor sensor)
      {
	    	  	boolean ok = true;
	    	  	if (categoryFragile)
	    	  		ok &= ruleFragile(sensor);
	    	  	if (categoryMedicinal)
	    	  		ok &= ruleMedicinal(sensor);
	    	  	if (categoryPerishable)
	    	  		ok &= rulePerishable(sensor);
	    	  	if (categoryEdible)
	    	  		ok &= ruleEdible(sensor);
	    	  	if (categoryDry)
	    	  		ok &= dry(sensor);
	    	  	if (temp != null)
	    	  		ok &= tempCheck(sensor);
	    	  	
	    	  	return ok;
      }
      
      // this version returns String of all violations (false) or "" (true)
      public String violatedSensorCatRules(Sensor sensor)
      {
	    	  	boolean a = true;
	    	  	String s = "";
	    	  	if (categoryFragile)
	    	  	{
	    	  		a = ruleFragile(sensor);
	    	  		if (!a)
	    	  			s += "fragile ";	
	    	  	}
	    	  	if (categoryMedicinal)
	    	  	{
	    	  		a = ruleMedicinal(sensor);
	    	  		if (!a)
	    	  			s += "medicinal ";
	    	  	}
	    	  	if (categoryPerishable)
	    	  	{
	    	  		a = rulePerishable(sensor);
	    	  		if (!a)
	    	  			s += "perishable ";
	    	  	}
	    	  	if (categoryEdible)
	    	  	{
	    	  		a = ruleEdible(sensor);
	    	  		if (!a)
	    	  			s += "edible ";
	    	  	}
	    	  	if (categoryDry)
	    	  	{
	    	  		a = dry(sensor);
	    	  		if (!a)
	    	  			s += "dry ";
	    	  	}
	    	  	if (temp != null)
	    	  	{
	    	  		a = tempCheck(sensor);
	    	  		if (!a)
	    	  			s += "temp";
	    	  	}
	    	  	
	    	  	return s;
      }
      
      // check if goods are allowed on a truck given temp rules and truck temperature range
      public boolean truckTempRules(Trucks truck)
      {
	    	  	// if any temperature cat then check truck offering
	    	  	if (categoryAmbientTemp || categoryCoolTemp || categoryFrozenTemp || categoryHeatSensitiveTemp || categoryRoomTemp)
	    	  	{
	    	  		if (!truck.tempControlled)
	    	  			return false;
	    	  			
	    	  		// if needs to be frozen truck must be frozen etc
	    	  		if (categoryFrozenTemp)
	    	  			return truck.tempRange == 0;
	    	  		
	    	  		if (categoryHeatSensitiveTemp)
	    	  			return truck.tempRange == 1;
	    	  		
	    	  		if (categoryCoolTemp)
	    	  			return truck.tempRange == 2;
	    	  		
	    	  		if (categoryRoomTemp)
	    	  			return truck.tempRange == 3;
	    	  		
	    	  		// for ambient temp, any temp range except freezing is ok
	    	  		if (categoryAmbientTemp)
	    	  			return truck.tempRange == 1 || truck.tempRange == 2 || truck.tempRange == 3 || truck.tempRange == 4;
	    	  	}
	    	  	return true;
      }
      
      // Are Goods allowed in warehouse given temperature rules?
	  public boolean warehouseTempRules(Warehouses truck)
	  {
	  		// if any temp cat then check warehouse temperature offering
		  	if (categoryAmbientTemp || categoryCoolTemp || categoryFrozenTemp || categoryHeatSensitiveTemp || categoryRoomTemp)
		  	{
		  		if (!truck.tempControlled)
		  			return false;
		  			
		  		// if needs to be frozen truck must be frozen
		  		if (categoryFrozenTemp)
		  			return truck.tempRange == 0;
		  		
		  		if (categoryHeatSensitiveTemp)
		  			return truck.tempRange == 1;
		  		
		  		if (categoryCoolTemp)
		  			return truck.tempRange == 2;
		  		
		  		if (categoryRoomTemp)
		  			return truck.tempRange == 3;
		  		
		  		if (categoryAmbientTemp)
		  			return truck.tempRange == 1 || truck.tempRange == 2 || truck.tempRange == 3 || truck.tempRange == 4;
		  	}
		  	return true;
	  }   

	  // return String of Goods categories and properties
      public String toStr()
      {
              String s = "";
              s += "RFID tag=" + tag + ", ";
              s += categoryPerishable ? " category Perishable, " : "";
              s += categoryHazardous ? " category Hazardous, " : "";
              s += categoryFragile ? " category Fragile, " : "";
              s += categoryEdible ? " category Edible, " : "";
              s += categoryBulky ? " category Bulky, " : "";
              s += weight + "kg, ";
              s += quantity + " items, ";
              s += height + "cm height, ";
              s += width + "cm width, ";
              s += breadth + "cm breadth, ";
              s += volume + "cubiccm volume, ";
              s += weight + "kg weight";
              return s;
       }

       public static void main(String[] args)
       {
              for (int i=0; i < 100; i++)
              {
                     Goods g = new Goods();
                     String s = g.toStr();
                     System.out.println(s);
                     System.out.println("Categories= " + g.categories);
              }
       }

}