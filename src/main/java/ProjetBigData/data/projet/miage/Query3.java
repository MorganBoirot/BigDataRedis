package ProjetBigData.data.projet.miage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

public class Query3 {

	Jedis jedis = new Jedis();
	
	public Query3() {}
	
	public ArrayList<String> query3(String produit, String dateD, String dateF) throws ParseException {
		Entry<String, StreamEntryID> streamFeedback = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Feedback", new StreamEntryID());
		Entry<String, StreamEntryID> streamInvoices = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Invoice", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamI = jedis.xread(693910, 1L, streamInvoices);
		List<Entry<String, List<StreamEntry>>> streamF = jedis.xread(150000, 1L, streamFeedback);
		
		ArrayList<String> list = new ArrayList<String>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = sdf.parse(dateD);
		Date date2 = sdf.parse(dateF);
		
		
		for(StreamEntry streamUneLigneI : streamI.get(0).getValue()){
			if(!streamUneLigneI.getFields().get("Orderline.asin").equals(produit))
				continue;
			for(StreamEntry streamUneLigneF : streamF.get(0).getValue()){
				
				if(streamUneLigneF.getFields().get("asin").equals(produit) && streamUneLigneI.getFields().get("PersonId").equals(streamUneLigneF.getFields().get("PersonId"))) {
					Date dateT = sdf.parse(streamUneLigneI.getFields().get("OrderDate"));
					
					if(!(dateT.compareTo(date2) > 0 || dateT.compareTo(date) < 0))
						if(streamUneLigneF.getFields().get("feedback").startsWith(" 1") || streamUneLigneF.getFields().get("feedback").startsWith(" 2")) {
						list.add(streamUneLigneF.getFields().get("feedback"));
					}
				}	
			}
		}
		
		System.out.println(list.toString());
		return list;
	}
	
}
