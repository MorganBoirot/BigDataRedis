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

public class Query5 {

	Jedis jedis = new Jedis();
	
	public Query5() {}
	
	public ArrayList<String> query5(String person, String cat) {
		
		Entry<String, StreamEntryID> streamInvoice = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Invoice", new StreamEntryID());
		Entry<String, StreamEntryID> streamPersonKnows = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("person_knows", new StreamEntryID());
		Entry<String, StreamEntryID> streamFeedback = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Feedback", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamF = jedis.xread(150000, 1L, streamFeedback);
		List<Entry<String, List<StreamEntry>>> streamI = jedis.xread(693910, 1L, streamInvoice);
		List<Entry<String, List<StreamEntry>>> streamPK = jedis.xread(187810, 1L, streamPersonKnows);
		
		List<String> ami1 = new ArrayList<String>();
		
		ArrayList<String> feedback = new ArrayList<String>();
		
		for(StreamEntry streamUneLignePK : streamPK.get(0).getValue()){
	    	if(streamUneLignePK.getFields().get("Person.idk").equals(person)) {
	    		ami1.add(streamUneLignePK.getFields().get("Person.id"));
	    		
	    		for(StreamEntry streamUneLignePK2 : streamPK.get(0).getValue()){
	    			
	    			if(streamUneLignePK2.getFields().get("Person.idk").equals(streamUneLignePK.getFields().get("Person.id"))) {
	    				if( !(ami1.contains(streamUneLignePK2.getFields().get("Person.id"))) && (streamUneLignePK2.getFields().get("Person.id") != person)) {
	    					ami1.add(streamUneLignePK2.getFields().get("Person.id"));
	    					
	    					for(StreamEntry streamUneLignePK3 : streamPK.get(0).getValue()){
	    						
	    						if(streamUneLignePK3.getFields().get("Person.idk").equals(streamUneLignePK2.getFields().get("Person.id"))) 
	    		    				if( !(ami1.contains(streamUneLignePK3.getFields().get("Person.id"))) && (streamUneLignePK3.getFields().get("Person.id") != person)) 
	    		    					ami1.add(streamUneLignePK3.getFields().get("Person.id"));
	    					}
	    				}
	    			}
	    		}
	    	}
		}	
		
		Map<String,String> myMap = new HashMap<String, String>();
		
		for (StreamEntry streamUneLigneI : streamI.get(0).getValue()) {
			if(ami1.contains(streamUneLigneI.getFields().get("PersonId")) && streamUneLigneI.getFields().get("Orderline.brand").equals(cat))
				myMap.put(streamUneLigneI.getFields().get("PersonId"), streamUneLigneI.getFields().get("Orderline.asin"));
		}
		
		for (StreamEntry streamUneLigneF : streamF.get(0).getValue()) {
			if(myMap.containsKey(streamUneLigneF.getFields().get("PersonId")) && myMap.containsValue(streamUneLigneF.getFields().get("asin")))
				if(streamUneLigneF.getFields().get("feedback").startsWith(" 5.0"))
				feedback.add(streamUneLigneF.getFields().get("feedback"));
		}
		
		for (String string : feedback) {
			System.out.println("Feedback: " + string);
		}
		
		return feedback;
		
	}
}
