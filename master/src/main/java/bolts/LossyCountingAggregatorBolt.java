package bolts;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import data.LossyCountingElement;
import util.Util;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

/**
 * Created by ctebbe
 */
public class LossyCountingAggregatorBolt extends BaseBasicBolt {

    private final int topN;
    private int n = 0;

    public LossyCountingAggregatorBolt(int topN) {
        this.topN = topN;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //List<LossyCountingElement> elements = (List<LossyCountingElement>) tuple.getValueByField("results");
        PrintWriter writer = null;
        try {
            writer = new PrintWriter((n++) + "top_tweets.txt","UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        writer.println(tuple);
        writer.flush();
        writer.close();

        if(Util.isTickTuple(tuple)) {
            writeResults(basicOutputCollector);
        } else {

        }
    }

    private void writeResults(BasicOutputCollector collector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }
}
