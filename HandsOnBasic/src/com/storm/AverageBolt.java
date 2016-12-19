package com.storm;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class AverageBolt implements IRichBolt{
	private OutputCollector collector;
	private HashMap<String, Integer> map;
	private BufferedWriter fw;
	private final static String FILE_NAME = "output.txt";
	private int idx = 0;
	
	@Override
	public void cleanup() {
		
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		boolean isSum = tuple.getFields().get(0).equals("sum");
		String ID = tuple.getString(1);
		int value = tuple.getInteger(0);
		if(map.containsKey(ID)){
			idx++;
			if(isSum){
				try {
					fw.write("The average at time " + idx + " is equal to " + (double) value / (double) map.get(ID));
					fw.newLine();
					fw.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else{
				try {
					fw.write("The average at time " + idx + " is equal to " + (double) map.get(ID) / (double) value);
					fw.newLine();
					fw.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			map.remove(ID);
		}	
		else{
			map.put(ID, value);
		}
		collector.ack(tuple);
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		// TODO Auto-generated method stub
		this.collector = collector;
		this.map = new HashMap<String, Integer>();
		try {
			this.fw = new BufferedWriter(new FileWriter(FILE_NAME));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("sum"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
