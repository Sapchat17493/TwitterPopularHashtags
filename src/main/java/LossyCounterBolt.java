import clojure.lang.Cons;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LossyCounterBolt extends BaseRichBolt {
    private final boolean DEBUG = Constants.DEBUG;

    private OutputCollector outputCollector;
    private double epsilon;
    private double threshold;
    private ConcurrentHashMap<String, LossyCounters> allBuckets;
    private long bucketNumber = 1; //1, not 0
    private int currentBucketCount;
    private int bucketCapacity;

    LossyCounterBolt(double epsilon, double threshold) {
        if (epsilon < 0.5) {
            this.epsilon = epsilon;
        } else {
            if (DEBUG) {
                System.out.println("Invalid value of epsilon. Please provide a valid value (ideally around or below 0.02). Setting epsilon to 0.01");
                this.epsilon = 0.01;
            }
        }
        this.threshold = threshold;
        allBuckets = new ConcurrentHashMap<String, LossyCounters>();
        bucketCapacity = (int) Math.round(1 / this.epsilon);
        currentBucketCount = 0;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String hashtag = tuple.getStringByField("HashTag");
        if(!Constants.CLUSTER_MODE) {
            System.out.println("Found Hashtag in Counter Bolt : " + hashtag);
        }

        if (!hashtag.isEmpty() && hashtag.startsWith("#")) {
            lossyCounting(hashtag.toLowerCase());
        } else {
            if (DEBUG) {
                System.out.println("Found empty string or a string not beginning with #");
            }
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashtag", "counts"));
    }

    private void lossyCounting(String hashtag) {
        if (currentBucketCount < bucketCapacity) {
            if (allBuckets.containsKey(hashtag)) {
                LossyCounters counters = allBuckets.get(hashtag);
                counters.setFrequency(counters.getFrequency() + 1);
                allBuckets.put(hashtag, counters);
            } else {
                LossyCounters counters = new LossyCounters(1, bucketNumber - 1);
                allBuckets.put(hashtag, counters);
            }
            currentBucketCount++;
        } else if (currentBucketCount == bucketCapacity) {
            cleanBuckets();
            int frequencyThreshold = Math.round((float) (threshold - epsilon) / allBuckets.size());
            if (DEBUG) {
                System.out.println("Frequency Threshold : " + frequencyThreshold);
            }
            for (String tag : allBuckets.keySet()) {
                LossyCounters counters = allBuckets.get(tag);
                if ((counters.getFrequency() + counters.getMaxError()) >= frequencyThreshold) {
                    if (DEBUG) {
                        System.out.println("Emitting Hashtag : " + tag + " with counts (Frequency : " + counters.getFrequency() + " and MaxError : " + counters.getMaxError());
                    }
                    outputCollector.emit(new Values(tag, counters.getFrequency() + counters.getMaxError()));
                }
            }
            currentBucketCount = 0;
            bucketNumber++;
        } else {
            if (DEBUG) {
                System.out.println("Should not happen. Thrown from Lossy Counter Bolt (current bucket count > bucket capacity)");
            }
        }
    }

    private void cleanBuckets() {
        for (String tag : allBuckets.keySet()) {
            LossyCounters counters = allBuckets.get(tag);
            if (counters.getFrequency() + counters.getMaxError() <= bucketNumber) {
                allBuckets.remove(tag);
            }
        }
    }
}
