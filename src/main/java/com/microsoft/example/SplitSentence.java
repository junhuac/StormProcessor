package com.microsoft.example;

import java.text.BreakIterator;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class SplitSentence extends BaseBasicBolt {

  //Execute is called to process tuples
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    //Get the device id from the tuple
    Integer id = tuple.getInteger(0);
    collector.emit(new Values(id));
  }

  //Declare that emitted tuples will contain a word field
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id"));
  }
}
