package com.storm;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import twitter4j.Status;

public class LanguageFilter extends BaseFilter{
    private static final Logger LOG = LoggerFactory.getLogger(LanguageFilter.class);

	public boolean isKeep(TridentTuple tuple){
		try{
			// Get the language value
			Status status = (Status) tuple.get(0);
			
			// return true only if the language is one of the requested ones.
			return status.getLang().equals("en") || status.getLang().equals("ja")
					|| status.getLang().equals("es") || status.getLang().equals("ar")
					|| status.getLang().equals("el") || status.getLang().equals("fr")
					|| status.getLang().equals("it") || status.getLang().equals("pt")
					|| status.getLang().equals("tr") || status.getLang().equals("ms");
		} catch(Exception e){
			LOG.error("Error: " + e.getMessage());
			return false;
		}
	}
}
