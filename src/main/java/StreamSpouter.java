import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class StreamSpouter extends BaseRichSpout {
    private final boolean DEBUG = Constants.DEBUG;

    private SpoutOutputCollector spoutOutputCollector;
    private LinkedBlockingQueue<Status> streamQueue;
    private TwitterStream stream;

    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        streamQueue = new LinkedBlockingQueue<Status>();

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(Constants.API_KEY)
                .setOAuthConsumerSecret(Constants.API_SECRET_KEY)
                .setOAuthAccessToken(Constants.ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(Constants.ACCESS_TOKEN_SECRET);


        StatusListener statusListener = new StatusListener() {
            public void onStatus(Status status) {
                if (DEBUG) {
                    System.out.println("Adding new status to Stream Queue");
                }
                streamQueue.add(status);
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
                if (DEBUG) {
                    e.printStackTrace();
                }
            }
        };


        stream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
        stream.addListener(statusListener);
        stream.sample("en");
    }

    public void nextTuple() {
        Status nextStatus = streamQueue.poll();

        if (nextStatus != null) {
            if (DEBUG) {
                System.out.println(nextStatus.getText());
            }
            spoutOutputCollector.emit(new Values(nextStatus));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweetz"));
    }

    @Override
    public void close() {
        stream.shutdown();
        super.close();
    }
}
