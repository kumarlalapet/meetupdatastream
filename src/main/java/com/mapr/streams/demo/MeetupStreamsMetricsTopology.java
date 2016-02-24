package com.mapr.streams.demo;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.*;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by mlalapet on 2/23/16.
 */
public class MeetupStreamsMetricsTopology {

    public static final String COUNT_FIELD = "count";
    public static final String MESSAGE_FIELD = "message";

    public static void main(String args[]) throws IOException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();

        Properties props = new Properties();
        props.load(MeetupStreamsMetricsTopology.class.getResourceAsStream("/kafka.properties"));
        props.put("topology.message.timeout.secs", 60);

        String zkConnString = args[0]; //localhost:5181
        String topicName = args[1];

        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, topicName , UUID.randomUUID().toString());
        spoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new KafkaBoltKeyValueScheme());
        spoutConfig.kafkaAPIv="0.9";

        KafkaSpout spout = new KafkaSpout(spoutConfig);
        builder.setSpout("spout", spout, 1);

        long tuplesCountPeriodInSecs = 10;

        CountMetricsBolt countBolt = new CountMetricsBolt(tuplesCountPeriodInSecs);
        builder.setBolt("countBolt", countBolt, 1).globalGrouping("spout");

        StormSubmitter.submitTopology("StreamsMetrics", props, builder.createTopology());
    }

    public static class KafkaBoltKeyValueScheme extends StringKeyValueScheme {
        @Override
        public Fields getOutputFields() {
            return new Fields("message");
        }
    }

}
