package storm;

import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import com.google.gson.Gson;
// import com.google.gson.GsonBuilder;

public class ParserBolt extends BaseBasicBolt {
  private static final Logger LOG = LoggerFactory.getLogger(ParserBolt.class);

  //Declare output fields & streams
  //hbasestream is all fields, and goes to hbase
  //dashstream is just the device and temperature, and goes to the dashboard
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("hbasestream", new Fields("timestamp", "deviceid", "latitude", "longitude"));
    declarer.declareStream("dashstream", new Fields("deviceid", "latitude", "longitude"));
  }

  //Process tuples
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    //Should only be one tuple, which is the JSON message from the spout
    String value = tuple.getString(0);
    LOG.info("Read tuple {} from stream.", value);
    //Deal with cases where we get multiple
    //EventHub messages in one tuple
    String[] arr = value.split("}");
    for (String ehm : arr)
    {
        String timestamp = "";
        int deviceid = 0;
        double latitude = 0.0;
        double longitude = 0.0;

        //Convert it from JSON to an object
        //EventHubMessage msg = new Gson().fromJson(ehm.concat("}"),EventHubMessage.class);
        try {
        JSONObject msg=new JSONObject(ehm.concat("}"));
        //Pull out the values and emit as a stream
        timestamp = msg.getString("timestamp");
        deviceid = msg.getInt("deviceid");
        latitude = msg.getDouble("latitude");
        longitude = msg.getDouble("longitude");
        } catch (Exception e) {
            //Handle JSONException
        }

        LOG.info("Emitting device id {} with a latitude of {}, longitude of {}, and timestamp of {}", deviceid, latitude, longitude, timestamp);

        collector.emit("hbasestream", new Values(timestamp, deviceid, latitude, longitude));
        collector.emit("dashstream", new Values(deviceid, latitude, longitude));
    }
  }
}
