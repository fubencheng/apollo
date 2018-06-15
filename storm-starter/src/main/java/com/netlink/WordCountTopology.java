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

    public static void main(String[] args) throws Exception {

        RandomSentenceSpout sentenceSpout = new RandomSentenceSpout();
        SplitSentenceBolt sentenceBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(SENTENCE_SPOUT_ID, sentenceSpout);
        topologyBuilder.setBolt(SPLIT_BOLT_ID, sentenceBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
        topologyBuilder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        topologyBuilder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        config.setDebug(false);

        if(args != null && args.length > 0){
            // 集群模式
            config.setNumWorkers(2);
            // 启动命令中指定拓扑名
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        } else {
            // 单机模式
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());
        }

    }
}
