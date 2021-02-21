import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import bolts.HashtagExtractBolt;
import bolts.HashtagsExtractLogBolt;
import bolts.LossyCountingAggregatorBolt;
import bolts.LossyCountingAlgorithmBolt;
import spout.TwitterSpout;
import twitter4j.FilterQuery;

/**
 * Created by ct.
 */
public class MainTopology {

    public static void main(String[] args) {
        FilterQuery filter = null; //new FilterQuery();
        //filter.track(new String[]{"#"});

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TwitterSpout(filter));
        //builder.setBolt("log", new HashtagsExtractLogBolt("hashtags.log")).shuffleGrouping("spout");

        builder.setBolt("hashtag", new HashtagExtractBolt()).shuffleGrouping("spout");
        builder.setBolt("lossy-algo", new LossyCountingAlgorithmBolt(0.2), 1).fieldsGrouping("hashtag", new Fields("hashtag"));
        builder.setBolt("aggregator", new LossyCountingAggregatorBolt(100)).globalGrouping("lossy-algo");

        Config conf = new Config();
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("twitter", conf, builder.createTopology());
        Utils.sleep(50000);
        //cluster.shutdown();
    }
}
