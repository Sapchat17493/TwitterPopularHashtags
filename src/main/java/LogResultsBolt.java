import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class LogResultsBolt extends BaseRichBolt {
    private final boolean DEBUG = Constants.DEBUG;

    private OutputCollector outputCollector;
    private LinkedBlockingQueue<Tuple> hashtagWithCounts;
    private int intervalInSeconds;
    private long timestamp;
    private String logFile;
    private PrintWriter pw;
    private Timer timer;
    private int numberOfHashtags;

    LogResultsBolt(String logFile, int intervalInSeconds, int numberOfHashtags) {
        this.intervalInSeconds = intervalInSeconds;
        hashtagWithCounts = new LinkedBlockingQueue<Tuple>();
        this.logFile = logFile;
        this.timestamp = System.currentTimeMillis();
        this.numberOfHashtags = numberOfHashtags;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;


        try {
            FileWriter fileWriter = new FileWriter(logFile, true); //Set true for append mode
            pw = new PrintWriter(fileWriter);
        } catch (FileNotFoundException e) {
            if (DEBUG) {
                System.out.println("FileName/FilePath for logging results is invalid");
                e.printStackTrace();
            }
        } catch (IOException e) {
            if (DEBUG) {
                System.out.println("Result Logfile absent");
                e.printStackTrace();
            }
        }

        TimerTask tsk = new TimerTask() {
            @Override
            public void run() {
                String s = "<" + timestamp + "> ";

                List<Tuple> sortableList = new ArrayList<Tuple>();
                hashtagWithCounts.drainTo(sortableList);

                sortableList.sort(new Comparator<Tuple>() {
                    @Override
                    public int compare(Tuple t1, Tuple t2) {
                        if (t1.getLongByField("counts") > t2.getLongByField("counts")) {
                            return -1;
                        } else if (t1.getLongByField("counts") < t2.getLongByField("counts")) {
                            return 1;
                        } else {
                            return 0;
                        }
                    }
                });

                int hashtagCount = 0;
                if (sortableList.size() > numberOfHashtags) {
                    hashtagCount = numberOfHashtags;
                } else {
                    hashtagCount = sortableList.size();
                }

                for (int i = 0; i < hashtagCount; i++) {
                    Tuple t = sortableList.get(i);
                    if(Constants.CLUSTER_MODE){
                        s += "<" + t.getStringByField("hashtag") + "> ";
                    } else {
                        s += "<" + t.getStringByField("hashtag") + " " + t.getLongByField("counts") + "> ";
                    }
                }

                if (DEBUG) {
                    System.out.println("LOG LINE: " + s);
                }

                if (s.contains("#")) {
                    pw.println(s);
                    //pw.flush();
                }
                timestamp = System.currentTimeMillis();
            }
        };

        timestamp = System.currentTimeMillis();
        timer = new Timer("Interval Tracker");
        timer.scheduleAtFixedRate(tsk, 0, intervalInSeconds * 1000);
    }

    @Override
    public void execute(Tuple tuple) {
        if (notContains(tuple)) {
            hashtagWithCounts.add(tuple);
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // No output fields to declare
    }

    private boolean notContains(Tuple tuple) {
        Tuple[] temp = new Tuple[hashtagWithCounts.size()];
        Tuple[] allTuples = hashtagWithCounts.toArray(temp);

        for (Tuple tup : allTuples) {
            if (tuple.getStringByField("hashtag").equalsIgnoreCase(tup.getStringByField("hashtag"))) {
                hashtagWithCounts.remove(tup);
                hashtagWithCounts.add(tuple);
                return false;
            }
        }

        return true;
    }

    @Override
    public void cleanup() {
        pw.close();
        super.cleanup();
    }
}
