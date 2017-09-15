package com.test.paasstorm.bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class CustomBolt extends BaseBasicBolt {
    public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        System.out.println(sentence);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.out.println("declareOutputFields");
    }
}
