package ProjetBigData.data.projet.miage;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

public class Query9 {
	Jedis jedis = new Jedis();
	
	public Query9() {}
	
	public Map<String, ArrayList<String>> query9(String country) {
		
		Entry<String, StreamEntryID> streamBrand = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Brand", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamB = jedis.xread(9694, 1L, streamBrand);
		Entry<String, StreamEntryID> streamVendor = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Vendor", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamV = jedis.xread(66, 1L, streamVendor);
		Entry<String, StreamEntryID> streamInvoice = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Invoice", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamI = jedis.xread(693910, 1L, streamInvoice);
		
		ArrayList<String> marques = new ArrayList<String>();
		
		for(StreamEntry streamUneLigneV : streamV.get(0).getValue()) {
			if(streamUneLigneV.getFields().get("Country").equals(country))
				marques.add(streamUneLigneV.getFields().get("Vendor"));
		}
		
		System.out.println("Les marques présentes sont " + marques.toString());

		Map<String, ArrayList<String>> myMap = new HashMap<String, ArrayList<String>>();
		Map<String,Integer> achats = new HashMap<String, Integer>();
		
		for (String marque : marques) {
			for(StreamEntry streamUneLigneB : streamB.get(0).getValue()) {
				if(!streamUneLigneB.getFields().get("brand").equals(marque)) 
					continue;
				
				for(StreamEntry streamUneLigneI : streamI.get(0).getValue()) {
					
					String str = streamUneLigneB.getFields().get("asin");
					str = str.substring(0, str.length()-1);
					
					if(streamUneLigneI.getFields().get("Orderline.asin").equals(str)) {
						if(myMap.containsKey(streamUneLigneB.getFields().get("brand"))){
							ArrayList<String> x = myMap.get(streamUneLigneB.getFields().get("brand"));
							x.add(streamUneLigneI.getFields().get("PersonId"));
							myMap.remove(streamUneLigneB.getFields().get("brand"));
							myMap.put(streamUneLigneB.getFields().get("brand"), x);
							
							int i = achats.get(streamUneLigneB.getFields().get("brand"));
							achats.remove(streamUneLigneB.getFields().get("brand"));
							achats.put(streamUneLigneB.getFields().get("brand"), i+1);
							
						}
				    	else {
				    		ArrayList<String> acheteurs = new ArrayList<String>();
				    		acheteurs.add(streamUneLigneI.getFields().get("PersonId"));
				    		myMap.put(streamUneLigneB.getFields().get("brand"), acheteurs);
				    		
				    		achats.put(streamUneLigneB.getFields().get("brand"),1);
				    	}
					}
				}
				
			}
		}
		
		int max = 0;
		int max2 = 0;
		int max3 = 0;
		String best = "";
		String best2 = "";
		String best3 = "";
		
		for (Entry<String, Integer> entry : achats.entrySet()) {
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
		
		System.out.println("La marque qui vend le plus " + best + " avec " + max);
		System.out.println("La 2eme marque qui vend le plus " + best2 + " avec " + max2);
		System.out.println("La 3eme marque qui vend le plus " + best3 + " avec " + max3);
		
		Map<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		map.put(best, myMap.get(best));
		map.put(best2, myMap.get(best2));
		map.put(best3, myMap.get(best3));
		
		return map;
	}
	
	public void query92(Map<String, ArrayList<String>> map) {
		
		Entry<String, StreamEntryID> streamCustomer = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Customer", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamC = jedis.xread(9949, 1L, streamCustomer);
		
		Map<String, Integer> ratioF = new HashMap<String, Integer>();
		Map<String, Integer> ratioH = new HashMap<String, Integer>();
		
		for(Entry<String, ArrayList<String>> entry : map.entrySet()){
			for(StreamEntry streamUneLigneC : streamC.get(0).getValue()) {
				if(entry.getValue().contains(streamUneLigneC.getFields().get("id"))) {
					if(streamUneLigneC.getFields().get("gender").equals("female")) {
						if(ratioF.containsKey(entry.getKey())) {
							int x = ratioF.get(entry.getKey());
							ratioF.remove(entry.getKey());
							ratioF.put(entry.getKey(), x+1);
						}else {
							ratioF.put(entry.getKey(), 1);
						}
					}else {
						if(ratioH.containsKey(entry.getKey())) {
							int x = ratioH.get(entry.getKey());
							ratioH.remove(entry.getKey());
							ratioH.put(entry.getKey(), x+1);
						}else {
							ratioH.put(entry.getKey(), 1);
						}
					}
				}
			}
		}
		
		for (Entry<String, Integer> entry : ratioF.entrySet()) {
			System.out.println("Il y a " + entry.getValue() + " acheteuses chez " + entry.getKey());
		}
		
		for (Entry<String, Integer> entry : ratioH.entrySet()) {
			System.out.println("Il y a " + entry.getValue() + " acheteurs chez " +  entry.getKey());
		}
		
		
	}
	
	public Map<String, ArrayList<String>> query93(Map<String, ArrayList<String>> map) {
		Entry<String, StreamEntryID> streamPostCreator = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("post_hasCreator", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamPC = jedis.xread(1231991, 1L, streamPostCreator);
		
		Map<String, ArrayList<String>> postCreator = new HashMap<String,ArrayList<String>>();
		
		for(Entry<String, ArrayList<String>> entry : map.entrySet()){
			for(StreamEntry streamUneLignePC : streamPC.get(0).getValue()) {
				if(entry.getValue().contains(streamUneLignePC.getFields().get("Person.id"))) {
					if(postCreator.containsKey(streamUneLignePC.getFields().get("Person.id"))) {
						ArrayList<String> x = postCreator.get(streamUneLignePC.getFields().get("Person.id"));
						x.add(streamUneLignePC.getFields().get("Post.id"));
						postCreator.remove(streamUneLignePC.getFields().get("Person.id"));
						postCreator.put(streamUneLignePC.getFields().get("Person.id"), x);
					}else {
						ArrayList<String> x = new ArrayList<String>();
						x.add(streamUneLignePC.getFields().get("Post.id"));
						postCreator.put(streamUneLignePC.getFields().get("Person.id"), x);
					}
				}
			}
		}
		System.out.println("done");
		return postCreator;	
	}
	
	public void query94(Map<String, ArrayList<String>> postCreator) throws ParseException {
		Entry<String, StreamEntryID> streamPost = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Post", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamP = jedis.xread(1231991, 1L, streamPost);
		Map<String, StreamEntry> post = new HashMap<String, StreamEntry>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");//0-9
		
		for(Entry<String, ArrayList<String>> entry : postCreator.entrySet()){
			for(StreamEntry streamUneLigneP : streamP.get(0).getValue()) {
				if(entry.getValue().contains(streamUneLigneP.getFields().get("id"))){
					if(post.containsKey(entry.getKey())) {
						StreamEntry x = post.get(entry.getKey());
						Date date = sdf.parse(x.getFields().get("creationDate").substring(0, 9));
						Date date2 = sdf.parse(streamUneLigneP.getFields().get("creationDate").substring(0, 9));
						if(date.compareTo(date2) < 0) {
							x = streamUneLigneP;
							post.remove(entry.getKey());
							post.put(entry.getKey(), x);
						}
					}else {
						StreamEntry x = streamUneLigneP;
						post.put(entry.getKey(), x);
					}
				}
			}
		}
		
		for (Entry<String, StreamEntry> entry : post.entrySet()) {
			System.out.println(entry.getKey() + " a comme post le plus récent: " + entry.getValue().getFields().get("content") );
		}
	}
}
