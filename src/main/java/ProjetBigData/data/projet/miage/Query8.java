package ProjetBigData.data.projet.miage;

import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

public class Query8 {
	Jedis jedis = new Jedis();
	
	public Query8() {}
	
	public ArrayList<String> query8(String category, String annee) {
		
		Entry<String, StreamEntryID> streamInvoice = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Invoice", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamI = jedis.xread(693910, 1L, streamInvoice);
		Entry<String, StreamEntryID> streamProduct = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Product", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamP = jedis.xread(8272, 1L, streamProduct);
		
		BigDecimal total = BigDecimal.ZERO;
		
		ArrayList<String> produits = new ArrayList<String>();
		ArrayList<String> nomProduits = new ArrayList<String>();
		
		for (StreamEntry streamUneLigneI : streamI.get(0).getValue()) {
			if(streamUneLigneI.getFields().get("Orderline.brand").contains(category) && streamUneLigneI.getFields().get("OrderDate").startsWith(annee)) {
				total = total.add(new BigDecimal(streamUneLigneI.getFields().get("Orderline.price")));
				if(!(produits.contains(streamUneLigneI.getFields().get("Orderline.asin")))) {
					produits.add(streamUneLigneI.getFields().get("Orderline.asin"));
				}
			}
		}
		
		System.out.println("La catégorie a produit " + total + " de chiffre");
		
		
		for (StreamEntry streamUneLigneP : streamP.get(0).getValue()) {
			if(produits.contains(streamUneLigneP.getFields().get("asin")))
				nomProduits.add(streamUneLigneP.getFields().get("title"));
		}
		
		return nomProduits;
	}
	
	public void query82(ArrayList<String> nomProduits, String annee) {
		Entry<String, StreamEntryID> streamPost = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("Post", new StreamEntryID());
		List<Entry<String, List<StreamEntry>>> streamPo = jedis.xread(1231991, 1L, streamPost);
		
		int nbPost = 0;
		
		System.out.println("Titre " + nomProduits.toString());
		
		for (StreamEntry streamUneLigneP : streamPo.get(0).getValue()) {
			if(!(streamUneLigneP.getFields().get("creationDate").startsWith(annee)))
				continue; 

			for(String nom : nomProduits) {
				if(streamUneLigneP.getFields().get("content").contains(nom))
					nbPost ++;
			}
		}
		
		System.out.println("il y a " + nbPost + " sur cette catégorie durant cette année");
		
	}
}
