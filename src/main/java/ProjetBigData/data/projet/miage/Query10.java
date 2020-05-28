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

public class Query10 {
	Jedis jedis = new Jedis();
	
	public Query10() {}
	
	public Map<String, ArrayList<String>> query10(String annee) {
		Entry<String, StreamEntryID> streamInvoice = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Invoice", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamI = jedis.xread(693910, 1L, streamInvoice);
		
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		Map<String, ArrayList<String>> map2 = new HashMap<String, ArrayList<String>>();
		
		for(StreamEntry streamUneLigneI : streamI.get(0).getValue()) {
			if(streamUneLigneI.getFields().get("OrderDate").startsWith(annee)) {
				if(map.containsKey(streamUneLigneI.getFields().get("PersonId"))){
					int x = map.get(streamUneLigneI.getFields().get("PersonId"));
					ArrayList<String> l = map2.get(streamUneLigneI.getFields().get("PersonId"));
					l.add(streamUneLigneI.getFields().get("Orderline.asin"));
					map2.remove(streamUneLigneI.getFields().get("PersonId"));
					map2.put(streamUneLigneI.getFields().get("PersonId"), l);
					map.remove(streamUneLigneI.getFields().get("PersonId"));
					map.put(streamUneLigneI.getFields().get("PersonId"), x+1);
				}
				else {
					ArrayList<String> l = new ArrayList<String>();
					l.add(streamUneLigneI.getFields().get("Orderline.asin"));
					map.put(streamUneLigneI.getFields().get("PersonId"), 1);
					map2.put(streamUneLigneI.getFields().get("PersonId"), l);
				}
			}
		}
		int max = 0;
		int max2 = 0;
		int max3 = 0;
		int max4 = 0;
		int max5 = 0;
		int max6 = 0;
		int max7 = 0;
		int max8 = 0;
		int max9 = 0;
		int max10 = 0;
		String best = "";
		String best2 = "";
		String best3 = "";
		String best4 = "";
		String best5 = "";
		String best6 = "";
		String best7 = "";
		String best8 = "";
		String best9 = "";
		String best10 = "";
		for (Entry<String, Integer> entry : map.entrySet()) {
	    	if(max < entry.getValue()){
	    		best = entry.getKey();
	    		max = entry.getValue();
	    	}else if(max2 < entry.getValue()) {
	    		best2 = entry.getKey();
	    		max2 = entry.getValue();
	    	}else if(max3 < entry.getValue()) {
	    		best3 = entry.getKey();
	    		max3 = entry.getValue();
	    	}else if(max4 < entry.getValue()) {
	    		best4 = entry.getKey();
	    		max4 = entry.getValue();
	    	}else if(max5 < entry.getValue()) {
	    		best5 = entry.getKey();
	    		max5 = entry.getValue();
	    	}else if(max6 < entry.getValue()) {
	    		best6 = entry.getKey();
	    		max6 = entry.getValue();
	    	}else if(max7 < entry.getValue()) {
	    		best7 = entry.getKey();
	    		max7 = entry.getValue();
	    	}else if(max8 < entry.getValue()) {
	    		best8 = entry.getKey();
	    		max8 = entry.getValue();
	    	}else if(max9 < entry.getValue()) {
	    		best9 = entry.getKey();
	    		max9 = entry.getValue();
	    	}else if(max10 < entry.getValue()) {
	    		best10 = entry.getKey();
	    		max10 = entry.getValue();
	    	}
		}
		
		Map<String, ArrayList<String>> meilleurs = new HashMap<String, ArrayList<String>>();
		meilleurs.put(best, map2.get(best));
		meilleurs.put(best2, map2.get(best2));
		meilleurs.put(best3, map2.get(best3));
		meilleurs.put(best4, map2.get(best4));
		meilleurs.put(best5, map2.get(best5));
		meilleurs.put(best6, map2.get(best6));
		meilleurs.put(best7, map2.get(best7));
		meilleurs.put(best8, map2.get(best8));
		meilleurs.put(best9, map2.get(best9));
		meilleurs.put(best10, map2.get(best10));
		
		for (Entry<String, ArrayList<String>> entry : meilleurs.entrySet()) {
			System.out.println(entry.getKey() + " a un des 10 meilleurs RFM avec " + entry.getValue().size() + " achats");
		}
		
		return meilleurs;
	}
	
	public void query102(Map<String, ArrayList<String>> best) {
		Entry<String, StreamEntryID> streamPersonHasInteres = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("person_hasInteres", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamPHI = jedis.xread(693910, 1L, streamPersonHasInteres);
		
		for (StreamEntry streamUneLignePHI : streamPHI.get(0).getValue()) {
			for (Entry<String, ArrayList<String>> entry : best.entrySet()) {
				if(streamUneLignePHI.getFields().get("Person.id").equals(entry.getKey()))
					System.out.println(entry.getKey() + " a un interet envers le Tag " + streamUneLignePHI.getFields().get("Tag.id"));
			}
		}
		
	}
	
	public void query103(Map<String, ArrayList<String>> best) {
		Entry<String, StreamEntryID> streamFeedback = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Feedback", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamF = jedis.xread(150000, 1L, streamFeedback);
		
		for (StreamEntry streamUneLigneF : streamF.get(0).getValue()) {
			for (Entry<String, ArrayList<String>> entry : best.entrySet()) {
				if(streamUneLigneF.getFields().get("PersonId").equals(entry.getKey()) && entry.getValue().contains(streamUneLigneF.getFields().get("asin"))) {
					System.out.println(entry.getKey() + " a recemment commenté: " + streamUneLigneF.getFields().get("feedback"));
				}
			}
		}
		
	}
	
}
