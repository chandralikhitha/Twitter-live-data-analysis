package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by ct.
 */
public class HashtagExtractBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String text = tuple.getStringByField("message");
        StringTokenizer st = new StringTokenizer(text);
        while(st.hasMoreElements()) {
            String tmp = (String) st.nextElement();
            if(StringUtils.startsWith(tmp, "#")) { // emit hashtag values
                collector.emit(new Values(tmp));
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashtag"));
    }
}
