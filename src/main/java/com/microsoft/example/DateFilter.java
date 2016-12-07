package com.microsoft.example;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import twitter4j.Status;

public class DateFilter extends BaseFilter{
    private static final Logger LOG = LoggerFactory.getLogger(DateFilter.class);

	public boolean isKeep(TridentTuple tuple){
		try{
			/*SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Status status = (Status) tuple.get(0);
			String createdAt = formatter.format(status.getCreatedAt());
			LOG.debug(createdAt);
			Date max = formatter.parse("2016-12-04");
			Date min = formatter.parse("2016-11-04");
			return true;
			//return formatter.parse(createdAt).before(max) && formatter.parse(createdAt).after(min);*/
			Status status = (Status) tuple.get(0);
			return status.getLang().equals("en") || status.getLang().equals("zh");
		} catch(Exception e){
			LOG.error("Error: " + e.getMessage());
			return false;
		}
	}
}
