package com.netlink;

import com.netlink.bolts.ReportBolt;
import com.netlink.bolts.SplitSentenceBolt;
import com.netlink.bolts.WordCountBolt;
import com.netlink.spouts.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

/**
 * WordCountTopology.
 *
 * @author fubencheng.
 * @version 0.0.1 2018-06-11 23:30 fubencheng.
 */
public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    /**
     * worker是进程，executor对应于线程，spout或bolt是一个个的task
     * 一个worker进程（jvm进程）执行的是一个topology的子集（spout，bolt，ack），不会存在一个worker为多个topology服务
     * 一个worker进程会启动一个或多个executor线程来执行一个topology的spout或者bolt
     * 默认情况下，一个supervisor节点会启动4个worker进程；每个worker进程会启动1个executor，每个executor启动1个task
     * 默认情况下，一个spout或一个bolt都只会生成一个task，Executor线程会在每次循环的时候顺序的去调用所有的task的实例
     * 调整并行度主要是调整executor的数量，并且调整之后的executor的数量必须小于等于task的数量
     * @param args args
     * @throws Exception Exception
     */
    public static void main(String[] args) throws Exception {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // 并发数即executors数设置为2，即线程数为2，task为4，即4个spout实例
        topologyBuilder.setSpout(SENTENCE_SPOUT_ID, new RandomSentenceSpout(), 2).setNumTasks(4);
        topologyBuilder.setBolt(SPLIT_BOLT_ID, new SplitSentenceBolt(), 4).setNumTasks(8).shuffleGrouping(SENTENCE_SPOUT_ID);
        topologyBuilder.setBolt(COUNT_BOLT_ID, new WordCountBolt(), 6).setNumTasks(8).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        topologyBuilder.setBolt(REPORT_BOLT_ID, new ReportBolt(), 8).setNumTasks(12).globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        config.setDebug(false);

        if(args != null && args.length > 0){
            // 集群模式，设置topology的worker数
            config.setNumWorkers(4);
            config.setMaxTaskParallelism(32);
            // 启动命令中指定拓扑名
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        } else {
            // 单机模式
            config.setMaxTaskParallelism(4);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());

            TimeUnit.MINUTES.sleep(3);

            cluster.shutdown();
        }

    }
}
