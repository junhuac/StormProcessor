package storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.Config;
import org.apache.storm.Constants;

import io.socket.client.IO;
import io.socket.client.Socket;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Map;

public class DashboardBolt extends BaseBasicBolt {
  //Socket.IO
  private Socket socket;
  private static String clientId;
  private static String redirectUri;
  private static String resourceUri;
  private static String authority;
  private static String datasetsUri;

  private static final Logger LOG = LoggerFactory.getLogger(DashboardBolt.class);

  //Declare output fields
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //no stream output - we talk directly to socket.io
  }

  @Override
  public void prepare(Map config, TopologyContext context) {
    //using Socket.io
    try {
      //Open a socket to your web server
      socket = IO.socket("http://localhost:3000");
      socket.connect();
    } catch(URISyntaxException e) {
      //Assume we can connect
    }
  }

  //Process tuples
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    //Get the device ID and temperature
    int deviceid = tuple.getIntegerByField("deviceid");
    int latitude = tuple.getIntegerByField("latitude");
    int longitude = tuple.getIntegerByField("longitude");

    LOG.info("DeviceID is {}, latitude is {}, longitude is {}", deviceid, latitude, longitude);
    //Create a JSON object
    JSONObject obj = new JSONObject();

    try {
    obj.put("deviceid", deviceid);
    obj.put("latitude", latitude);
    obj.put("longitude", longitude);
    } catch (Exception e) {
      //Handle JSON exception
    }
    //Send it to the server
    socket.emit("message", obj);
  }
}
