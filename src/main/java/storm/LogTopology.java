package storm;

import java.util.UUID;
import java.io.FileReader;
import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.StringScheme;

import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;

import org.apache.storm.eventhubs.samples.EventCount;
import org.apache.storm.eventhubs.spout.EventHubSpout;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;

import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.tuple.Fields;

//hbase
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.bolt.HBaseBolt;
import java.util.Map;
import java.util.HashMap;

public class LogTopology {
    protected EventHubSpoutConfig spoutConfig;
    protected SpoutConfig kafkaspoutConfig;
    protected int numWorkers;

    protected void readEHConfig(String[] args) throws Exception {
/*
        Properties properties = new Properties();
        if (args.length > 1) {
            properties.load(new FileReader(args[1]));
        } else {
            properties.load(EventCount.class.getClassLoader()
                    .getResourceAsStream("Config.properties"));
        }

        String username = properties.getProperty("eventhubspout.username");
        String password = properties.getProperty("eventhubspout.password");
        String namespaceName = properties
                .getProperty("eventhubspout.namespace");
        String entityPath = properties.getProperty("eventhubspout.entitypath");
*/
        String username = "zonar";
        String password = "qb5otAE6g7vyyU60uBHbWS50B8+xBK7oeW+gUO9rixc=";
        String namespaceName = "dtna-ns";
        String entityPath = "dtna";

/*
        String zkEndpointAddress = properties
                .getProperty("zookeeper.connectionstring");
        int partitionCount = Integer.parseInt(properties
                .getProperty("eventhubspout.partitions.count"));
        int checkpointIntervalInSeconds = Integer.parseInt(properties
                .getProperty("eventhubspout.checkpoint.interval"));
        int receiverCredits = Integer.parseInt(properties
                .getProperty("eventhub.receiver.credits"));
*/

        String zkEndpointAddress = "localhost:2181";
        int partitionCount = 4;
        int checkpointIntervalInSeconds = 10;
        int receiverCredits = 2;
                                                            
        System.out.println("Eventhub spout config: ");
        System.out.println("  partition count: " + partitionCount);
        System.out.println("  checkpoint interval: "
                + checkpointIntervalInSeconds);
        System.out.println("  receiver credits: " + receiverCredits);

        spoutConfig = new EventHubSpoutConfig(username, password,
                namespaceName, entityPath, partitionCount, zkEndpointAddress,
                checkpointIntervalInSeconds, receiverCredits);

        //spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        BrokerHosts hosts = new ZkHosts(zkEndpointAddress);

        String topicName = "device";
        kafkaspoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        kafkaspoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        numWorkers = spoutConfig.getPartitionCount();

        if (args.length > 0) {
            spoutConfig.setTopologyName(args[0]);
        }
    }

    protected StormTopology buildTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        EventHubSpout eventHubSpout = new EventHubSpout(spoutConfig);
        topologyBuilder.setSpout("EventHubsSpout", eventHubSpout,
                spoutConfig.getPartitionCount()).setNumTasks(
                spoutConfig.getPartitionCount());

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaspoutConfig);

        topologyBuilder.setSpout("KafkaSpout", kafkaSpout,
                spoutConfig.getPartitionCount()).setNumTasks(
                spoutConfig.getPartitionCount());

        topologyBuilder.setBolt("Parser", new ParserBolt(), spoutConfig.getPartitionCount())
                .localOrShuffleGrouping("KafkaSpout").setNumTasks(spoutConfig.getPartitionCount());

        topologyBuilder
                .setBolt("LoggerBolt", new LoggerBolt(),
                        spoutConfig.getPartitionCount())
                .localOrShuffleGrouping("KafkaSpout")
                .setNumTasks(spoutConfig.getPartitionCount());

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
          .withRowKeyField("deviceid")
          .withColumnFields(new Fields("timestamp", "latitude", "longitude"))
          .withColumnFamily("cf");

        // Create the HBase bolt, which subscribes to the stream from Parser
        topologyBuilder
                .setBolt("HBase", new HBaseBolt("DeviceData", mapper).withConfigKey("hbase.conf"), spoutConfig.getPartitionCount())
                .fieldsGrouping("Parser", "hbasestream", new Fields("deviceid")).setNumTasks(spoutConfig.getPartitionCount());

        //Config conf = new Config();
        //set producer properties.
        Properties props = new Properties();
        props.put("acks", "1");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("metadata.fetch.timeout.ms", 1000);
        //props.put("metadata.broker.list", "localhost:9092");
        //props.put("request.required.acks", "1");
        //props.put("serializer.class", "kafka.serializer.StringEncoder");
        //conf.put("kafka.broker.properties", props);

        KafkaBolt bolt = new KafkaBolt()
                     .withProducerProperties(props)
                     .withTopicSelector(new DefaultTopicSelector("device"))
                     .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        topologyBuilder.setBolt("forwardToKafka", bolt, 8).shuffleGrouping("EventHubsSpout");

        return topologyBuilder.createTopology();
    }

    protected void runScenario(String[] args) throws Exception {
        boolean runLocal = true;
        //If there are arguments, we are running on a cluster
        if (args != null && args.length > 0) {
            //runLocal = false;
        }
        readEHConfig(args);
        Config config = new Config();
        config.setDebug(false);

        Map<String, Object> hbConf = new HashMap<String, Object>();
        //if(args.length > 0) {
          hbConf.put("hbase.rootdir", "file:///tmp/hbase" /*args[0]*/);
        //}
        config.put("hbase.conf", hbConf);
        
        StormTopology topology = buildTopology();

        if (runLocal) {
            config.setMaxTaskParallelism(2);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("test", config, topology);
            Thread.sleep(5000000);
            localCluster.shutdown();
        } else {
            config.setNumWorkers(numWorkers);
            StormSubmitter.submitTopology(args[0], config, topology);
        }
    }

    public static void main(String[] args) throws Exception {
        LogTopology topology = new LogTopology();
        topology.runScenario(args);
    }
}

