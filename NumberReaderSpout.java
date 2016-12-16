import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class NumberReaderSpout implements IRichSpout{
	
	private SpoutOutputCollector collector;
	private boolean completed = false;
	private TopologyContext context;
	private Integer idx = 0;
	private Random randomGenerator = new Random();
	
	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}
	
	public boolean isDistributed(){
		return false;
	}
	
	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
			if(idx < 100000){
				idx++;
	        	int number;
	        	do{
	        		number = randomGenerator.nextInt(31);
	        	}while(number < 18);
	        	this.collector.emit(new Values(number, UUID.randomUUID().toString()));
			}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.context = context;
		this.collector = collector;
		System.out.println("open method for Spout has been called");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("number", "ID"));	
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
