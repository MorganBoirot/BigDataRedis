package ProjetBigData.data.projet.miage;

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

public class Query7 {

	Jedis jedis = new Jedis();
	
	public Query7() {}
	
	public void query7() throws ParseException {
		//Signia_(sportswear)
		
		Entry<String, StreamEntryID> streamBrand = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Brand", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamB = jedis.xread(9694, 1L, streamBrand);
		Entry<String, StreamEntryID> streamInvoice = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Invoice", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamI = jedis.xread(693910, 1L, streamInvoice);
		Entry<String, StreamEntryID> streamFeedback = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Feedback", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamF = jedis.xread(150000, 1L, streamFeedback);
		
		String vendeur = "Elfin_Sports_Cars";
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date dateT = sdf.parse("2019-08-06");
		
		int annee = dateT.getYear()+1900;
		int annee2 = dateT.getYear()+1900;
		String[] trimAct = {};
		String[] trimPrec = {};
		
		if(dateT.getMonth() == 0 || dateT.getMonth() == 1 || dateT.getMonth() == 2) {
			String[] trimAct1 = {"-01","-02","-03"};
			String[] trimPrec1 = {"-10","-11","-12"};
			annee2--;
			trimAct = trimAct1;
			trimPrec = trimPrec1;
		}else if(dateT.getMonth() == 3 || dateT.getMonth() == 4 || dateT.getMonth() == 5) {
			String[] trimAct1 = {"-04","-05","-06"};
			String[] trimPrec1 = {"-01","-02","-03"};
			trimAct = trimAct1;
			trimPrec = trimPrec1;
		}else if(dateT.getMonth() == 6 || dateT.getMonth() == 7 || dateT.getMonth() == 8) {
			String[] trimAct1 = {"-07","-08","-09"};
			String[] trimPrec1 = {"-04","-05","-06"};
			trimAct = trimAct1;
			trimPrec = trimPrec1;
		}else if(dateT.getMonth() == 9 || dateT.getMonth() == 10 || dateT.getMonth() == 11) {
			String[] trimAct1 = {"-10","-11","-12"};
			String[] trimPrec1 = {"-07","-08","-09"};
			trimAct = trimAct1;
			trimPrec = trimPrec1;
		}

		Map<String, Integer> venteAct = new HashMap<String, Integer>();
		Map<String, Integer> ventePrec = new HashMap<String, Integer>();
		

		for(StreamEntry streamUneLigneB : streamB.get(0).getValue()) {
			if(streamUneLigneB.getFields().get("brand").equals(vendeur)) {
				
				for(StreamEntry streamUneLigneI : streamI.get(0).getValue()) {
						
					String str = streamUneLigneB.getFields().get("asin");
					str = str.substring(0, str.length()-1);
					
					if(streamUneLigneI.getFields().get("Orderline.asin").equals(str)) {
						
						if( streamUneLigneI.getFields().get("OrderDate").startsWith((annee + trimAct[0])) ||streamUneLigneI.getFields().get("OrderDate").startsWith((annee + trimAct[1])) ||streamUneLigneI.getFields().get("OrderDate").startsWith((annee + trimAct[2])) ) {
							if(venteAct.containsKey(streamUneLigneI.getFields().get("Orderline.asin"))){
								int x = venteAct.get(streamUneLigneI.getFields().get("Orderline.asin"));
								venteAct.remove(streamUneLigneI.getFields().get("Orderline.asin"));
								venteAct.put(streamUneLigneI.getFields().get("Orderline.asin"), x+1);
							}
							else {
								venteAct.put(streamUneLigneI.getFields().get("Orderline.asin"), 1);
							}
							
						}else if( streamUneLigneI.getFields().get("OrderDate").startsWith((annee2 + trimPrec[0])) ||streamUneLigneI.getFields().get("OrderDate").startsWith((annee2 + trimPrec[1])) ||streamUneLigneI.getFields().get("OrderDate").startsWith((annee2 + trimPrec[2])) ) {
							if(ventePrec.containsKey(streamUneLigneI.getFields().get("Orderline.asin"))){
								int x = ventePrec.get(streamUneLigneI.getFields().get("Orderline.asin"));
								ventePrec.remove(streamUneLigneI.getFields().get("Orderline.asin"));
								ventePrec.put(streamUneLigneI.getFields().get("Orderline.asin"), x+1);
							}
							else {
								ventePrec.put(streamUneLigneI.getFields().get("Orderline.asin"), 1);
							}
							
						}
					}
				}
			}
		}
		
		ArrayList<String> mauvaiseVente = new ArrayList<String>();
		
		for (Entry<String, Integer> entry : venteAct.entrySet()) {
	    	if(ventePrec.containsKey(entry.getKey()) && ventePrec.get(entry.getKey()) > entry.getValue()) {
	    		mauvaiseVente.add(entry.getKey());
	    	}
	    		
		}
		
		int i = 0;
		
		if(mauvaiseVente.size() != 0) {
			System.out.println("Les produits " + mauvaiseVente.toString() + " sont des mauvaises ventes");
			
			for(StreamEntry streamUneLigneF : streamF.get(0).getValue()) {
				if( mauvaiseVente.contains(streamUneLigneF.getFields().get("asin")) && ( streamUneLigneF.getFields().get("feedback").contains("1.0,") || streamUneLigneF.getFields().get("feedback").contains("2.0,") ) ) {
					System.out.println("Commentaire négatif pour le Produit " + streamUneLigneF.getFields().get("asin") + ": " + streamUneLigneF.getFields().get("feedback"));
					i++;
				}
			}
			
			if(i == 0) {
				System.out.println("il n'y a pas de mauvais commentaires sur les mauvaises ventes");
			}
			
		}else {
			System.out.println("Il n'y a aucune mauvaise vente");
		}
		
	}
}
