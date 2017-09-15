package com.test.paasstorm.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 日志解析类
 *
 */

public class LogParserBolt implements IRichBolt {

    private Pattern pattern;
    private OutputCollector  collector;

    public static final String TimeStrfield = "TimeStr";
    public static final String MethodNamefield = "MethodName";
    public static final String RunTimeStrfield = "RunTimeStr";
    public static final String Messagefield = "Message";

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        pattern = Pattern.compile("[a-zA-Z0-9]\\|$");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String webLog = input.getStringByField("msg");

        // 解析
        if(webLog!= null || !"".equals(webLog)){

            Matcher matcher = pattern.matcher(webLog);
            if(matcher.find()){
                //matcher.group(0);
                String TimeStr = matcher.group(1);

              /*  // 处理时间
                long timestamp = Long.parseLong(serverTimeStr);
                Date date = new Date();
                date.setTime(timestamp);

                DateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
                String dateStr = df.format(date);
                String day = dateStr.substring(0,8);
                String hour = dateStr.substring(0,10);
                String minute = dateStr ;*/

                String MethodName = matcher.group(2);


              //  String RunTimeStr = matcher.group(3);
              //  String Message = matcher.group(4);
              //  this.collector.emit(new Values(TimeStr));
                this.collector.emit(new Values(MethodName));
               // this.collector.emit(new Values(RunTimeStr));
               // this.collector.emit(new Values(Message));

            }
        }

    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
     //   declarer.declare(new Fields(TimeStrfield));
        declarer.declare(new Fields(MethodNamefield));
      //  declarer.declare(new Fields(RunTimeStrfield));
     //   declarer.declare(new Fields(Messagefield));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


}
