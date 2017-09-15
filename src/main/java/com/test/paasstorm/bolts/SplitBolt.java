package com.test.paasstorm.bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitBolt extends BaseBasicBolt {

    public static final String field = "word";

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        String line = input.getStringByField("msg");
        // 按行来分割
        for (String word : line.split("\\|")) {
            // 传递给下一个组件，即 CountBolt
            if (word.contains("Hello")) {
                collector.emit(new Values(word));
            }
        }


    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(field));
    }
}