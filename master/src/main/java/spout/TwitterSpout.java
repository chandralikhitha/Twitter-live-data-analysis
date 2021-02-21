package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by ct.
 */
public class TwitterSpout extends BaseRichSpout {

    public static final String consumerKey = "";
    public static final String consumerSecret = "";
    public static final String accessToken = "";
    public static final String accessTokenSecret = "";

    private SpoutOutputCollector collector;
    private TwitterStream twStream;
    private FilterQuery tweetFilterQuery;
    private LinkedBlockingQueue msgs;

    public TwitterSpout(FilterQuery fq) {
        this.tweetFilterQuery = fq;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        msgs = new LinkedBlockingQueue();
        collector = spoutOutputCollector;
        ConfigurationBuilder confBuilder = new ConfigurationBuilder();
        confBuilder.setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret)
                ;//.setJSONStoreEnabled(true);
        twStream = new TwitterStreamFactory(confBuilder.build()).getInstance();
        twStream.addListener(new StatusListener() {
            public void onStatus(Status status) {
                msgs.offer(status.getText());
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onTrackLimitationNotice(int i) {

            }

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onException(Exception e) {

            }
        });
        if(tweetFilterQuery == null) {
            twStream.sample();
        } else {
            twStream.filter(tweetFilterQuery);
        }
    }

    public void nextTuple() {
        Object s = msgs.poll();
        if(s == null) {
            Utils.sleep(1000);
        } else {
            collector.emit(new Values(s));
        }
    }

    public void close() {
        twStream.shutdown();
        super.close();
    }
}
