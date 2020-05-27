package ProjetBigData.data.projet.miage;

import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

public class Query4 {

	Jedis jedis = new Jedis();
	
	public Query4() {}
	
	public void query4() {
		Entry<String, StreamEntryID> streamOrder = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Order", new StreamEntryID());
		Entry<String, StreamEntryID> streamPersonKnows = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("person_knows", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamO = jedis.xread(142257, 1L, streamOrder);
		List<Entry<String, List<StreamEntry>>> streamPK = jedis.xread(187810, 1L, streamPersonKnows);
		
		List<String> ami1 = new ArrayList<String>();
		List<String> ami2 = new ArrayList<String>();
		
		Map<String, BigDecimal> myMap = new HashMap<String, BigDecimal>();
		
		for(StreamEntry streamUneLigneO : streamO.get(0).getValue()){
			if(myMap.containsKey(streamUneLigneO.getFields().get("PersonId"))){
				BigDecimal x = myMap.get(streamUneLigneO.getFields().get("PersonId"));
				BigDecimal total = x.add(new BigDecimal(streamUneLigneO.getFields().get("TotalPrice")));
				myMap.remove(streamUneLigneO.getFields().get("PersonId"));
				myMap.put(streamUneLigneO.getFields().get("PersonId"), total);
			}
	    	else {
	    		myMap.put(streamUneLigneO.getFields().get("PersonId"), new BigDecimal(streamUneLigneO.getFields().get("TotalPrice")));
	    	}
		}
		
		
		BigDecimal max = new BigDecimal("0.0");
		BigDecimal max2 = new BigDecimal("0.0");
	    String best = "";
	    String best2 = "";
	    for (Entry<String, BigDecimal> entry : myMap.entrySet()) {
	    	if(max.compareTo(entry.getValue()) < 0){
	    		best = entry.getKey();
	    		max = entry.getValue();
	    	}else if(max2.compareTo(entry.getValue()) < 0) {
	    		max2 = entry.getValue();
	    		best2 = entry.getKey();
	    	}
		}
		
//	    System.out.println("1er " + best + " avec " + max);
//	    System.out.println("2eme " + best2 + " avec " + max2);
	    
	    for(StreamEntry streamUneLignePK : streamPK.get(0).getValue()){
	    	if(streamUneLignePK.getFields().get("Person.idk").equals(best)) {
	    		ami1.add(streamUneLignePK.getFields().get("Person.id"));
	    	}else if(streamUneLignePK.getFields().get("Person.idk").equals(best2)) {
	    		ami2.add(streamUneLignePK.getFields().get("Person.id"));
	    	}
	    }
	    
	    List<String> common = new ArrayList<String>(ami1);
	    common.retainAll(ami2);
	    
	    for(String p : common) {
			System.out.println(p + " est un ami commun aux deux plus gros acheteurs");
		}
	    
	    if(common.size() == 0)
	    	System.out.println("Les acheteurs n'ont aucun amis en commun !");
	}
}
