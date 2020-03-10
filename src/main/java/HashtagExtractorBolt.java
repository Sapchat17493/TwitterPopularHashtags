import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;
import java.util.StringTokenizer;

public class HashtagExtractorBolt extends BaseRichBolt {
    private final boolean DEBUG = Constants.DEBUG;

    private OutputCollector outputCollector;

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("tweetz");

        if (tweet != null) {
            if (DEBUG) {
                System.out.println(tweet.getText());
            }

            StringTokenizer tokenizer = new StringTokenizer(tweet.getText());
            String token = "";
            while (tokenizer.hasMoreTokens()) {
                token = tokenizer.nextToken();
                if (token.startsWith("#")) {
                    if (DEBUG) {
                        System.out.println("Found Hashtag " + token);
                    }

                    outputCollector.emit(new Values(token));
                }
            }
        }
        outputCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("HashTag"));
    }
}
