package com.test.paasstorm.bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseBasicBolt {


    public Logger log = LoggerFactory.getLogger(CountBolt.class);

    public static final String field = "result";

    private static Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        // 根据 field 获得上一个 bolt 传递过来的数据
        String word = input.getStringByField(SplitBolt.field);

        Integer count = counts.get(word);
        if (count == null)
            count = 0;
        count++;
        counts.put(word,count);

        String result = "方法 = " + word + ", 数目为 = " + count;
        collector.emit(new Values(result));
        log.info(result);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(field));
    }


}
