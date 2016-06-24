/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 /**
  * Original is available at https://github.com/apache/storm/blob/master/examples/storm-starter/src/jvm/storm/starter/spout/RandomSentenceSpout.java
  */

package com.microsoft.example;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

//This spout randomly emits sentences
public class RandomSentenceSpout extends BaseRichSpout {
  //Collector used to emit output
  SpoutOutputCollector _collector;
  //Used to generate a random number
  Random _rand;

  //Open is called when an instance of the class is created
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
  //Set the instance collector to the one passed in
    _collector = collector;
    //For randomness
    _rand = new Random();
  }

  //Emit data to the stream
  @Override
  public void nextTuple() {
  //Sleep for a bit
    Utils.sleep(100);
    //The device data that will be randomly emitted
    Integer[] ids = new Integer[]{0, 1, 2, 3, 4, 5, 6};
    Integer id = ids[_rand.nextInt(ids.length)];
    Float[] latitudes = new Float[]{1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
    Float latitude = latitudes[_rand.nextInt(latitudes.length)];
    Float[] longitudes = new Float[]{1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
    Float longitude = longitudes[_rand.nextInt(longitudes.length)];

    //Emit the device data 
    _collector.emit(new Values(id, latitude, longitude));
  }

  //Ack is not implemented since this is a basic example
  @Override
  public void ack(Object id) {
  }

  //Fail is not implemented since this is a basic example
  @Override
  public void fail(Object id) {
  }

  //Declare the output fields. In this case, an sentence
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id", "latitude", "longitude"));
  }
}
