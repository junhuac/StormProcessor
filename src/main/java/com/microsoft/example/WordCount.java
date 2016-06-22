package com.microsoft.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class WordCount extends BaseBasicBolt {
  //For holding words and counts
  Map<String, Integer> counts = new HashMap<String, Integer>();

  //execute is called to process tuples
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    //Get the word contents from the tuple
    String word = tuple.getString(0);
    //Have we counted any already?
    Integer count = counts.get(word);
    if (count == null)
      count = 0;
    //Increment the count and store it
    count++;
    counts.put(word, count);
    //Emit the word and the current count
    collector.emit(new Values(word, count));
  }

  //Declare that we will emit a tuple containing two fields; word and count
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count"));
  }
}
