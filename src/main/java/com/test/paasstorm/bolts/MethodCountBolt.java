package com.test.paasstorm.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class MethodCountBolt  implements IRichBolt {


    public Logger log = LoggerFactory.getLogger(CountBolt.class);

    public static final String Resultfield = "result";

    private static Map<String, Integer> counts = new HashMap<String, Integer>();

    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        // 根据 field 获得上一个 bolt 传递过来的数据
        String method = input.getStringByField(LogParserBolt.MethodNamefield);

        Integer count = counts.get(method);
        if (count == null)
            count = 0;
        count++;
        counts.put(method, count);

        String result = "方法" + method + ", count = " + count;
        this.collector.emit(new Values(method,count));
        log.info(result);
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("method","count"));
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
