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

public class Query6 {

	Jedis jedis = new Jedis();
	
	public Query6() {}
	
	public void query6(String person1, String person2) {
		Entry<String, StreamEntryID> streamInvoice = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Invoice", new StreamEntryID());
		Entry<String, StreamEntryID> streamPersonKnows = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("person_knows", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamPK = jedis.xread(187810, 1L, streamPersonKnows);
		List<Entry<String, List<StreamEntry>>> streamI = jedis.xread(142257, 1L, streamInvoice);
		
		List<String> recherche1 = new ArrayList<String>();
		List<String> recherche2 = new ArrayList<String>();
		
		List<String> ami1 = new ArrayList<String>();
		List<String> ami2 = new ArrayList<String>();
		
		recherche1.add(person1);
		recherche2.add(person2);
		
		int i = 0;
		
		ArrayList<String> common = new ArrayList<String>(ami1);
		    common.retainAll(ami2);
		
		while(common.size() == 0) {
			
			ami1.clear();
			ami2.clear();
			
			for (StreamEntry streamUneLignePK : streamPK.get(0).getValue()) {
				if(recherche1.contains(streamUneLignePK.getFields().get("Person.idk")))
					ami1.add(streamUneLignePK.getFields().get("Person.id"));
				else if(recherche2.contains(streamUneLignePK.getFields().get("Person.idk")))
					ami2.add(streamUneLignePK.getFields().get("Person.id"));
			}
			
			recherche1.clear();
			recherche2.clear();
			recherche1.addAll(ami1);
			recherche2.addAll(ami2);
			
			i++;
			
			common.clear();
			common.addAll(ami1);
			common.retainAll(ami2);
			
		}
		
		for (String string : common) {
			System.out.println(string + " est un ami en commun");
		}
		
		System.out.println("La boucle a descendu " + i + " fois.");
		
		Map<String, Integer> myMap = new HashMap<String, Integer>();
		
		for (StreamEntry streamUneLigneI : streamI.get(0).getValue()) {
			if(common.contains(streamUneLigneI.getFields().get("PersonId"))) {
				if(myMap.containsKey(streamUneLigneI.getFields().get("Orderline.productId"))){
					int x = myMap.get(streamUneLigneI.getFields().get("Orderline.productId"));
					myMap.remove(streamUneLigneI.getFields().get("Orderline.productId"));
					myMap.put(streamUneLigneI.getFields().get("Orderline.productId"), x+1);
				}
				else {
					myMap.put(streamUneLigneI.getFields().get("Orderline.productId"), 1);
				}
			}	
		}
		
		int max = 0;
		int max2 = 0;
		int max3 = 0;
		String best = "";
		String best2 = "";
		String best3 = "";
		for (Entry<String, Integer> entry : myMap.entrySet()) {
	    	if(max < entry.getValue()){
	    		best = entry.getKey();
	    		max = entry.getValue();
	    	}else if(max2 < entry.getValue()) {
	    		best2 = entry.getKey();
	    		max2 = entry.getValue();
	    	}else if(max3 < entry.getValue()) {
	    		best3 = entry.getKey();
	    		max3 = entry.getValue();
	    	}
		}
		
		System.out.println("Le produit " + best + " est le Best Seller");
		System.out.println("Le produit " + best2 + " est le 2eme Best Seller");
		System.out.println("Le produit " + best3 + " est le 3eme Best Seller");
		
	}
}
