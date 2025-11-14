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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StandaloneKinesisJob {
    private static final Logger LOG = LoggerFactory.getLogger(StandaloneKinesisJob.class);
    private static final String CONFIG_FILE = "flink-application-properties-dev.json";

    private static Properties loadPropertiesFromJson(String propertyGroupId) throws Exception {
        Properties properties = new Properties();
        
        try (InputStream inputStream = StandaloneKinesisJob.class.getClassLoader().getResourceAsStream(CONFIG_FILE);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            
            if (inputStream == null) {
                throw new RuntimeException("Configuration file not found: " + CONFIG_FILE);
            }
            
            StringBuilder jsonContent = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                jsonContent.append(line);
            }
            
            String json = jsonContent.toString();
            
            // Simple regex parsing to find the PropertyGroup
            Pattern groupPattern = Pattern.compile("\"PropertyGroupId\"\\s*:\\s*\"" + propertyGroupId + "\".*?\"PropertyMap\"\\s*:\\s*\\{([^}]+)\\}");
            Matcher groupMatcher = groupPattern.matcher(json);
            
            if (groupMatcher.find()) {
                String propertyMapContent = groupMatcher.group(1);
                
                // Parse key-value pairs
                Pattern kvPattern = Pattern.compile("\"([^\"]+)\"\\s*:\\s*\"([^\"]+)\"");
                Matcher kvMatcher = kvPattern.matcher(propertyMapContent);
                
                while (kvMatcher.find()) {
                    String key = kvMatcher.group(1);
                    String value = kvMatcher.group(2);
                    properties.setProperty(key, value);
                }
            }
        }
        
        // Map source.init.position to flink.stream.initpos for source config
        if (properties.containsKey("source.init.position")) {
            properties.setProperty("flink.stream.initpos", properties.getProperty("source.init.position"));
        }
        
        LOG.info("Loaded configuration for {}: {}", propertyGroupId, properties);
        return properties;
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Load stream configurations from JSON file
        Properties inputProperties = loadPropertiesFromJson("InputStream0");
        Properties outputProperties = loadPropertiesFromJson("OutputStream0");

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
