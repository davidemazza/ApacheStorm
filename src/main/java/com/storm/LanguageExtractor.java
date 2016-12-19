package com.storm;

import java.util.Map;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.Status;

public class LanguageExtractor extends BaseFunction {
	
  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    //Get the tweet
    final Status status = (Status) tuple.get(0);
    collector.emit(new Values(status.getLang()));
    //Loop through the hashtags
    /*for (HashtagEntity hashtag : status.getHashtagEntities()) {
      //Emit each hashtag
    	//if(hashtag.getText().toString().equals("iovotoNO") || hashtag.getText().toString().equals("iovotoSI"))
    	collector.emit(new Values(hashtag.getText()));
    }*/
  }
}
