package com.microsoft.example;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import twitter4j.Status;
import twitter4j.StatusUpdate;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class OutputTweet extends BaseFilter {
    private final String name;
    private int counter;
    private HashMap<String, Long> map;

    public OutputTweet() {
        name = "DEBUG: ";
        map=new HashMap();
    }

    public OutputTweet(String name) {
        this.name = "DEBUG(" + name + "): ";
    }
    
    private String codeToLang(String code){
    	if(code.equals("en"))
    		return "English";
    	else if (code.equals("es"))
    		return "Spanish";
    	else if (code.equals("ja"))
    		return "Japanese";
    	else if (code.equals("ar"))
    		return "Arabic";
    	else if (code.equals("el"))
    		return "Greek";
    	else if (code.equals("fr"))
    		return "French";
    	else if (code.equals("ja"))
    		return "Japanese";
    	else if (code.equals("it"))
    		return "Italian";
    	else if (code.equals("pt"))
    		return "Portoguese";
    	else if (code.equals("tr"))
    		return "Turkish";
    	else if (code.equals("ms"))
    		return "Malaysian";
    	return "";
    }
    
    private String printMap(){
    	String res = "Tweets: "+counter+"\n";
    	Collection<Long> values =map.values();
    	List list = new ArrayList(values);
    	Collections.sort(list, Collections.reverseOrder());
    	
    	for(long l : values){
    		for (String i : map.keySet()){
    			if(map.get(i).equals(l)){
    				res = res + i+ ": "+ l+"\n";
    			}
    		}
    	}
    	System.out.println("^^^^^^^^^^^^"+res.length());
    	return res;
    }
    @Override
    public boolean isKeep(TridentTuple tuple) {
    	counter++;
        System.out.println(name + tuple.toString()+" "+counter);
        map.put(tuple.getString(0), tuple.getLong(1));
        if (counter%100 == 0){
        	//Instantiate a re-usable and thread-safe factory
            TwitterFactory twitterFactory = new TwitterFactory();

            //Instantiate a new Twitter instance
            Twitter twitter = twitterFactory.getInstance();
        	System.out.println("--------------"+map.toString());
        	
        	//Instantiate and initialize a new twitter status update
        	String intro = "Apache Storm Tutorial: Twitter Most Popular Languages Now\n" ;
        	System.out.println("^^^^^^^^^"+intro.length());
            StatusUpdate statusUpdate = new StatusUpdate(intro + printMap());

            //tweet or update status
            try{
            	Status status = twitter.updateStatus(statusUpdate);
            }
            catch(TwitterException e){
            	System.out.println(e.getErrorMessage());
            }
        }
        return true;
    }
}

