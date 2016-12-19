package com.storm;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
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

    private int NUM_TWEETS = 100;
	private String INTRO = "Twitter Most Used Languages\n" ;
	// TODO replace with your name
	private String AUTHOR = "Roberto"; 

    public OutputTweet() {
        name = "DEBUG: ";
        map=new HashMap();
    }

    public OutputTweet(String name) {
        this.name = "DEBUG(" + name + "): ";
    }
    
    private String codeToLang(String code){
    	if(code.equals("en"))
    		return "🇬🇧";
    	else if (code.equals("es"))
    		return "🇪🇸";
    	else if (code.equals("ja"))
    		return "🇯🇵";
    	else if (code.equals("ar"))
    		return "🇦🇪";
    	else if (code.equals("el"))
    		return "🇬🇷";
    	else if (code.equals("fr"))
    		return "🇫🇷";
    	else if (code.equals("it"))
    		return "🇮🇹";
    	else if (code.equals("pt"))
    		return "🇵🇹";
    	else if (code.equals("tr"))
    		return "🇹🇷";
    	else if (code.equals("ms"))
    		return "🇲🇾";
    	return "";
    }
    
    private String printMap(){
    	String res = "Results over "+counter+" tweets:\n";
    	Object [] keys = map.keySet().toArray();
    	Arrays.sort(keys);
    	for (Object i : keys){
    		res = res + codeToLang((String)i)+ " "+ map.get(i)+"\n";	
    	}
    	return res;
    }
    @Override
    public boolean isKeep(TridentTuple tuple) {
    	counter++;
        System.out.println(name + tuple.toString()+" "+counter);
        map.put(tuple.getString(0), tuple.getLong(1));
        if (counter%NUM_TWEETS == 0){
        	//Instantiate a re-usable and thread-safe factory
            TwitterFactory twitterFactory = new TwitterFactory();

            //Instantiate a new Twitter instance
            Twitter twitter = twitterFactory.getInstance();
        	
        	//Instantiate and initialize a new twitter status update
            String tweetText = INTRO + printMap() + "Made by "+AUTHOR+".";
            System.out.println("Length: "+tweetText.length());
            StatusUpdate statusUpdate = new StatusUpdate(tweetText);

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



