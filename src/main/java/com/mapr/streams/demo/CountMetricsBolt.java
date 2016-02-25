package com.mapr.streams.demo;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by mlalapet on 2/23/16.
 */
public class CountMetricsBolt implements IRichBolt {

    protected OutputCollector collector;
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(CountMetricsBolt.class);
    private long periodInSecs;
    private volatile long count;

    public CountMetricsBolt(long pPeriodInSecs) {
        this.periodInSecs = pPeriodInSecs;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        if(isTickTuple(tuple)){
            long cachedCount = count;
            count = 0;
            log.info("Got tick tuple, emitting count={"+cachedCount+"}");
            collector.emit(tuple,new Values(String.valueOf(cachedCount)));
        } else {
            count++;
            Object value = null;
            Object message;

            if(tuple.contains(MeetupStreamsMetricsTopology.MESSAGE_FIELD)){
                value = tuple.getValueByField(MeetupStreamsMetricsTopology.MESSAGE_FIELD);
                if(value instanceof String){
                    message = ((String) value).getBytes();
                }else{
                    message = value;
                }
            }else{
                message = "".getBytes();
            }

            log.info("Increasing count={"+count+"} for message "+new String((byte[])message));

            collector.ack(tuple);
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(MeetupStreamsMetricsTopology.COUNT_FIELD));
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, periodInSecs);
        return conf;
    }

    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

}
