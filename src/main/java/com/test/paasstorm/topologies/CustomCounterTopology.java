package com.test.paasstorm.topologies;

import com.test.paasstorm.bolts.*;
import com.test.paasstorm.schemes.MessageScheme;
import com.test.paasstorm.util.PropertiesUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;
import java.util.Map;

public class CustomCounterTopology {



    /*
    * 入口类，任务提交类
    * @throws InterruptedException
    * @throws AlreadyAliveException
    * @throws InvalidTopologyException
    * */


    public static void main(String []args) throws AlreadyAliveException,InvalidTopologyException{
        System.out.println("11111");
        PropertiesUtil propertiesUtil = new PropertiesUtil("/application.properties", false);
        Map propsMap = propertiesUtil.getAllProperty();
        String zks = propsMap.get("zk_hosts").toString();
        String topic = propsMap.get("kafka.topic").toString();
        String zkRoot = propsMap.get("zk_root").toString();
        String zkPort = propsMap.get("zk_port").toString();
        String zkId = propsMap.get("zk_id").toString();
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, zkId);
        spoutConfig.zkServers = Arrays.asList(zks.split(","));
        if (zkPort != null && zkPort.length() > 0) {
            spoutConfig.zkPort = Integer.parseInt(zkPort);
        } else {
            spoutConfig.zkPort = 2181;
        }
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
       TopologyBuilder builder = new TopologyBuilder();
        // 设置 spout: KafkaSpout
        String Spout = "kafkaSpout";
        builder.setSpout(Spout, new KafkaSpout(spoutConfig),3);
        //builder.setBolt("customCounterBolt", new CustomBolt(), 1).shuffleGrouping("kafkaSpout");
    /*按word来计算*/
        // 设置 一级 bolt
        String Bolt1 = SplitBolt.class.getSimpleName();
        builder.setBolt(Bolt1, new SplitBolt(), 4) // 并行度 8
                .shuffleGrouping(Spout); // 上一级是 kafkaSpout, 随机分组

        // 设置 二级 bolt
        String Bolt2 = CountBolt.class.getSimpleName();
        builder.setBolt(Bolt2, new CountBolt(), 4) // 并行度 12
                .fieldsGrouping(Bolt1,new Fields(SplitBolt.field)); // 上一级是 WordSplitBolt, 按字段分组
       // 按method来计算
      // 设置 一级 bolt
       /* String Bolt1 = LogParserBolt.class.getSimpleName();
        builder.setBolt(Bolt1, new LogParserBolt(), 1) // 并行度 8
                .shuffleGrouping(Spout); // 上一级是 kafkaSpout, 随机分组

        // 设置 二级 bolt
        String Bolt2 = MethodCountBolt.class.getSimpleName();
        builder.setBolt(Bolt2, new MethodCountBolt(), 1) // 并行度 12
                .fieldsGrouping(Bolt1, new Fields("method")); // 上一级是 WordSplitBolt, 按字段分组
        //Configuration*/
        Config conf = new Config();
        conf.setDebug(false);
        if (args != null && args.length > 0) {
            //提交到集群运行
            try {
                StormSubmitter.submitTopologyWithProgressBar("CustomCounterTopology",conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            conf.setMaxTaskParallelism(3);
            //本地模式运行
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("CustomCounterTopology",conf,builder.createTopology());
        }




    }


   }
