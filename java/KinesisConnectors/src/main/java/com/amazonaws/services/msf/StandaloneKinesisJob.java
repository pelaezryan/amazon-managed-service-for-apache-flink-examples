package com.amazonaws.services.msf;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.shaded.guava31.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StandaloneKinesisJob {
    private static final Logger LOG = LoggerFactory.getLogger(StandaloneKinesisJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure input stream properties
        Properties inputProperties = new Properties();
        inputProperties.setProperty("stream.arn", "arn:aws:kinesis:us-east-1:123456789012:stream/test-flink-upstream-stream");
        inputProperties.setProperty("aws.region", "us-east-1");
        inputProperties.setProperty("flink.stream.initpos", "LATEST");
        
        // Configure output stream properties with aggressive flush settings
        Properties outputProperties = new Properties();
        outputProperties.setProperty("stream.arn", "arn:aws:kinesis:us-east-1:123456789012:stream/test-flink-downstream-stream");
        outputProperties.setProperty("aws.region", "us-east-1");
        outputProperties.setProperty("collection.max.count", "1");
        outputProperties.setProperty("aggregation.enabled", "false");
        outputProperties.setProperty("collection.max.size", "1");
        outputProperties.setProperty("flush.sync", "true");

        // Create Kinesis source
        KinesisStreamsSource<String> source = KinesisStreamsSource.<String>builder()
                .setStreamArn(inputProperties.getProperty("stream.arn"))
                .setSourceConfig(Configuration.fromMap(Maps.fromProperties(inputProperties)))
                .setDeserializationSchema(new SimpleStringSchema())
                .build();

        // Create Kinesis sink
        KinesisStreamsSink<String> sink = KinesisStreamsSink.<String>builder()
                .setStreamArn(outputProperties.getProperty("stream.arn"))
                .setKinesisClientProperties(outputProperties)
                .setSerializationSchema(new SimpleStringSchema())
                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                .build();

        // Read from input stream
        DataStream<String> input = env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "Kinesis Source")
                .returns(String.class);

        // Capitalize records and write to output stream
        input.map(record -> {
            LOG.info("Processing record: {}", record);
            return record.toUpperCase();
        }).sinkTo(sink);

        // Execute
        env.execute("Standalone Kinesis Job");
    }
}
