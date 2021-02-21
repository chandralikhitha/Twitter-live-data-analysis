package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import data.LossyCountingDataStructure;
import util.Util;

import java.util.Map;

/**
 * Created by ctebbe
 */
public class LossyCountingAlgorithmBolt extends BaseBasicBolt {

    private final LossyCountingDataStructure buckets;

    public LossyCountingAlgorithmBolt(double e) {
        buckets = new LossyCountingDataStructure((int) (1/e));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(Util.isTickTuple(tuple)) {
            emitResults(basicOutputCollector);
        } else {
            buckets.insert(tuple.getStringByField("hashtag"));
        }
    }

    private void emitResults(BasicOutputCollector collector) {
        collector.emit(new Values(buckets.getResults()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("results"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Util.getEmitFrequencyConfig();
    }
}
