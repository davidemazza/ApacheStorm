import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CounterBolt implements IRichBolt{
	private OutputCollector collector;
	private int result;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String ID = tuple.getString(1);
		result += 1;
		this.collector.emit(new Values(result, ID));
		collector.ack(tuple);
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		this.result = 0;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("counter", "ID"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
