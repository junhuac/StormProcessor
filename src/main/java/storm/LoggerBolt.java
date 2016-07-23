package storm;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class LoggerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static final Logger logger = LoggerFactory
              .getLogger(LoggerBolt.class);

    @Override
    public void execute(Tuple tuple) {
        String value = tuple.getString(0);
        logger.info("Tuple value: " + value);

        collector.ack(tuple);
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        // this.count = 0;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // no output fields
        }

}

