package com.mapr.streams.demo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;

/**
 * Created by mlalapet on 2/24/16.
 *
 * NOTE - the row key causes hot spotting. Its okay for this demo
 */
public class HBaseWriterBolt extends BaseRichBolt {

    public static final org.slf4j.Logger log = LoggerFactory.getLogger(HBaseWriterBolt.class);

    private static final String TABLE_NAME = "/tables/streammetrics";
    private static final String CF_NAME = "metricscf";
    private static final String QUAL_NAME = "count";
    private Configuration configuration;
    private HTable metricsTable;
    private String prefixKey;
    protected OutputCollector collector;

    @Override
    public void prepare(Map properties, TopologyContext topologyContext, OutputCollector outputCollector) {
        configuration = HBaseConfiguration.create();
        try {
            metricsTable = new HTable(configuration,TABLE_NAME);
        } catch (IOException e) {
            log.error("error in creating metrics table", e);
        }
        prefixKey = (String)properties.get(MeetupStreamsMetricsTopology.PREFIX_KEY);
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.contains(MeetupStreamsMetricsTopology.COUNT_FIELD)) {
            String countValue = tuple.getStringByField(MeetupStreamsMetricsTopology.COUNT_FIELD);
            //if(Integer.valueOf(countValue) > 0) {
                long reverseTimeStamp = Long.MAX_VALUE - System.currentTimeMillis();
                String rowKey = prefixKey + "_10S_" + reverseTimeStamp;

                Put put = new Put(Bytes.toBytes(rowKey));
                put.add(Bytes.toBytes(CF_NAME), Bytes.toBytes(QUAL_NAME), Bytes.toBytes(countValue));
                try {
                    metricsTable.put(put);
                } catch (InterruptedIOException e) {
                    log.error("error in writing to metrics table", e);
                } catch (RetriesExhaustedWithDetailsException e) {
                    log.error("error in writing to metrics table", e);
                }
                log.info("wrote successfully to hbase for " + rowKey + " value " + countValue);
            //} else {
            //    log.info("count value is 0 so no write to HBase");
            //}
            //collector.emit(tuple,new Values(String.valueOf(countValue).getBytes()));
        }
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        if(metricsTable != null) {
            try {
                metricsTable.close();
            } catch (IOException e) {
                log.error("error in closing metrics table", e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declare(new Fields(MeetupStreamsMetricsTopology.COUNT_FIELD));
    }

}
