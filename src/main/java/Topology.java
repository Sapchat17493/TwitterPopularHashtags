import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.IOException;

public class Topology {
    public static void main(String[] args) throws IOException {
        final boolean DEBUG = Constants.DEBUG;

        final String SPOUT = "twitter-spout";
        final String HASHTAG_EXTRACTOR_BOLT = "hashtag-extractor-bolt";
        final String LOSSY_COUNTER_BOLT = "lossy-counter-bolt";
        final String LOG_RESULTS_BOLT = "log-results-bolt";
        final String TOPOLOGY_NAME = "popular-hashtag-counter-and-updater";
        final long sleepingTime = 250000;

        Config conf = new Config();
        double epsilon = Double.parseDouble(args[0]);
        double threshold = Double.parseDouble(args[1]);
        String resultsLog = args[2];
        boolean PARALLEL_MODE = false;
        if(args.length > 3) {
            if (args[2].equalsIgnoreCase("Y")) {
                PARALLEL_MODE = true;
            }
        }

        TopologyBuilder builder = new TopologyBuilder();
        if (!PARALLEL_MODE) {
            builder.setSpout(SPOUT, new StreamSpouter());
            builder.setBolt(HASHTAG_EXTRACTOR_BOLT, new HashtagExtractorBolt()).shuffleGrouping(SPOUT);
            builder.setBolt(LOSSY_COUNTER_BOLT, new LossyCounterBolt(epsilon, threshold)).fieldsGrouping(HASHTAG_EXTRACTOR_BOLT, new Fields("HashTag"));
            builder.setBolt(LOG_RESULTS_BOLT, new LogResultsBolt(resultsLog, 10, 100)).globalGrouping(LOSSY_COUNTER_BOLT);
        } else {
            builder.setSpout(SPOUT, new StreamSpouter(), 1);
            builder.setBolt(HASHTAG_EXTRACTOR_BOLT, new HashtagExtractorBolt(), 8).shuffleGrouping(SPOUT);
            builder.setBolt(LOSSY_COUNTER_BOLT, new LossyCounterBolt(epsilon, threshold), 8).fieldsGrouping(HASHTAG_EXTRACTOR_BOLT, new Fields("HashTag"));
            builder.setBolt(LOG_RESULTS_BOLT, new LogResultsBolt(resultsLog, 10, 100), 1).globalGrouping(LOSSY_COUNTER_BOLT);
        }

        if (Constants.CLUSTER_MODE) {
            conf.setNumWorkers(16);
            try {
                StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                if (DEBUG) {
                    e.printStackTrace();
                }
            }
        } else {
            //conf.setMaxTaskParallelism(3);
            LocalCluster cluster = null;
            try {
                long start = System.currentTimeMillis(), end;
                cluster = new LocalCluster();
                cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());

                do {
                    end = System.currentTimeMillis();
                } while (end - start <= sleepingTime);

                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}