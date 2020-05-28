package ProjetBigData.data.projet.miage;

import java.util.ArrayList;
import java.util.Map;

public class QueryMain{
	
	public static void main( String args[] ) {  
	    try{
//	    	Query1 q1 = new Query1();
//	    	q1.query1("4145");
//	    	
//	    	Query2 q2 = new Query2();
//	    	q2.query2("B000PD9YTU","2023-05-03","2023-05-16");
//	    	
//	    	Query3 q3 = new Query3();
//	    	q3.query3("B003D9RBMU","2020-11-03","2020-11-20");
//	    	
//	    	Query4 q4 = new Query4();
//	    	q4.query4();
//	    	
//	    	Query5 q5 = new Query5();
//	    	q5.query5("10995116278711","MYLAPS_Sports_Timing\r");
//	    	
//	    	Query6 q6 = new Query6();
//	    	q6.query6("32985348843154", "10995116278711");
//	    	
//	    	Query7 q7 = new Query7();
//	    	q7.query7("Elfin_Sports_Cars","2019-08-06");
//	    	
//	    	Query8 q8 = new Query8();
//	    	ArrayList <String> l = q8.query8("Signia_(sportswear)", "2019");
//	    	q8.query82(l, "2019");
//	    	
//	    	Query9 q9 = new Query9();
//	    	Map<String, ArrayList<String>> map = q9.query9("United_States");
//	    	q9.query92(map);
//	    	Map<String, ArrayList<String>> x = q9.query93(map);
//	    	q9.query94(x);
//	    	
	    	Query10 q10 = new Query10();
	    	Map<String, ArrayList<String>> best = q10.query10("2019");
	    	q10.query102(best);
	    	q10.query103(best);
	    	
	    }
		catch(Exception e){
			e.printStackTrace();
		}	
	} 
	
}