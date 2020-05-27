package ProjetBigData.data.projet.miage;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

public class Query1 {
	Jedis jedis = new Jedis();
	
	public Query1() {}
	
	public String query1(String personId) {
			
			List<StreamEntry> streamList = new ArrayList<StreamEntry>();
			
			Map<String, Integer> myMap = new HashMap<String, Integer>();
			
			Map<String, Integer> myMap2 = new HashMap<String, Integer>();
			
			Entry<String, StreamEntryID> streamCustomer = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Customer", new StreamEntryID());
			Entry<String, StreamEntryID> streamOrder = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Order", new StreamEntryID());
			Entry<String, StreamEntryID> streamInvoices = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Invoice", new StreamEntryID());
			Entry<String, StreamEntryID> streamFeedback = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Feedback", new StreamEntryID());
			Entry<String, StreamEntryID> streamPostCreator = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("post_hasCreator", new StreamEntryID());
			Entry<String, StreamEntryID> streamPostTag = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("post_hasTag", new StreamEntryID());
				
			List<Entry<String, List<StreamEntry>>> streamC = jedis.xread(9949, 1L, streamCustomer);
			List<Entry<String, List<StreamEntry>>> streamO = jedis.xread(142257, 1L, streamOrder);
			List<Entry<String, List<StreamEntry>>> streamI = jedis.xread(693910, 1L, streamInvoices);
			List<Entry<String, List<StreamEntry>>> streamF = jedis.xread(150000, 1L, streamFeedback);
			List<Entry<String, List<StreamEntry>>> streamPC = jedis.xread(1231991, 1L, streamPostCreator);
			List<Entry<String, List<StreamEntry>>> streamPT = jedis.xread(1964328, 1L, streamPostTag);
			
	
	    	for(StreamEntry streamUneLignes : streamC.get(0).getValue()){   	
	    		if(streamUneLignes.getFields().get("id").equals(personId)) {
	    			streamList.add(streamUneLignes);
	    		}        		
	    	}
	    	
	    	for(StreamEntry streamUneLignes : streamO.get(0).getValue()){   	
	    		if(streamUneLignes.getFields().get("PersonId").equals(personId)) {
	    			streamList.add(streamUneLignes);
	    		}        		
	    	}
	    	
	    	for(StreamEntry streamUneLignes : streamI.get(0).getValue()){   	
	    		if(streamUneLignes.getFields().get("PersonId").equals(personId)) {
	    			streamList.add(streamUneLignes);
	    			
	    			if(myMap2.containsKey(streamUneLignes.getFields().get("Orderline.brand"))){
	    				int x = myMap2.get(streamUneLignes.getFields().get("Orderline.brand"));
	    				myMap2.remove(streamUneLignes.getFields().get("Orderline.brand"));
	    				myMap2.put(streamUneLignes.getFields().get("Orderline.brand"), x+1);
	    			}
	    			else {
	    				myMap2.put(streamUneLignes.getFields().get("Orderline.brand"), 1);
	    			}
	    		}        		
	    	}
	    	
	    	for(StreamEntry streamUneLignes : streamF.get(0).getValue()){   	
	    		if(streamUneLignes.getFields().get("PersonId").equals(personId)) {
	    			streamList.add(streamUneLignes);
	    		}        		
	    	}
			
	    	for(StreamEntry streamUneLignes : streamPC.get(0).getValue()){ 
	    		if(!streamUneLignes.getFields().get("Person.id").equals(personId))
					continue;
	    		for(StreamEntry streamUneLignes2 : streamPT.get(0).getValue()){   	
	    			if(streamUneLignes.getFields().get("Person.id").equals(personId) && streamUneLignes.getFields().get("Post.id").equals(streamUneLignes2.getFields().get("Post.id"))) {
	        			if(myMap.containsKey(streamUneLignes2.getFields().get("Tag.id"))){
	        				int x = myMap.get(streamUneLignes2.getFields().get("Tag.id"));
	        				myMap.remove(streamUneLignes2.getFields().get("Tag.id"));
	        				myMap.put(streamUneLignes2.getFields().get("Tag.id"), x+1);
	        			}
				    	else {
				    		myMap.put(streamUneLignes2.getFields().get("Tag.id"), 1);
				    	}
	        		}   
	        	}     		
	    	}
	    	
	    	
		    for(StreamEntry streamListe : streamList){ 
		    		System.out.println(streamListe);
		    }
		    
		    int max = 0;
		    String best = "";
		    for (Entry<String, Integer> entry : myMap.entrySet()) {
		    	if(max < entry.getValue()){
		    		best = entry.getKey();
		    		max = entry.getValue();}
			}
		    
		    System.out.println("Le Tag le plus utilisé est " + best + " avec des utilisations aux nombres de " + myMap.get(best));
		    
		    max = 0;
		    String best2 = "";
		    for (Entry<String, Integer> entry : myMap2.entrySet()) {
		    	if(max < entry.getValue()) {
		    		best2 = entry.getKey();	
		    		max = entry.getValue();
		    	}
			}
		    
		    System.out.println("Le catégorie la plus utilisée est " + best2 + " avec un nombre d'achats de " + myMap2.get(best2));
		    
		    return best2;
		}
}
