package com.jvo.viewlogreporting.config;

import com.jvo.viewlogkafkaproducer.dto.ViewLogDto;
import com.jvo.viewlogreporting.dto.CampaignDataDto;
import com.jvo.viewlogreporting.dto.EnrichedViewLogDto;
import com.jvo.viewlogreporting.dto.SerdeConfig;
import com.jvo.viewlogreporting.dto.ViewsReportDto;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;
import java.util.Properties;

@Configuration
public class ApplicationConfig {

    private final KafkaStreamsConfig kafkaStreamsConfig;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private String applicationId;
    private Map<String, String> properties;

    public ApplicationConfig(KafkaStreamsConfig kafkaStreamsConfig) {
        this.kafkaStreamsConfig = kafkaStreamsConfig;
    }


    @Bean
    public SerdeConfig serdeConfig() {

        JsonSerde<ViewLogDto> viewLogSerde = createJsonSerde(ViewLogDto.class, "com.jvo.viewlogkafkaproducer.dto.ViewLogDto");

        String trustedPackage = "com.jvo.viewlogreporting.dto";
        JsonSerde<CampaignDataDto> campaignDataSerde = createJsonSerde(CampaignDataDto.class, trustedPackage);
        JsonSerde<EnrichedViewLogDto> enrichedViewLogSerde = createJsonSerde(EnrichedViewLogDto.class, trustedPackage);
        JsonSerde<ViewsReportDto> viewsReportSerde = createJsonSerde(ViewsReportDto.class, trustedPackage);

        return new SerdeConfig(viewLogSerde, campaignDataSerde, enrichedViewLogSerde, viewsReportSerde);
    }

    private <T> JsonSerde<T> createJsonSerde(Class<T> classType, String trustedPackage) {
        JsonSerde<T> jsonSerde = new JsonSerde<>(classType);
        jsonSerde.deserializer().trustedPackages(trustedPackage);

        return jsonSerde;
    }

    @Bean(name = "enrichingStreamConfig")
    public Properties enrichingStreamConfig() {

        String streamId = kafkaStreamsConfig.getApplicationId() + "-enriching-stream";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, getTrustedPackages());

        Map<String, String> additionalProperties = kafkaStreamsConfig.getProperties();
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, additionalProperties.getOrDefault(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000"));
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, additionalProperties.getOrDefault(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1"));
        props.put(StreamsConfig.STATE_DIR_CONFIG, additionalProperties.getOrDefault(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams") + "/" + streamId);

        return props;
    }

    @Bean(name = "reportingStreamConfig")
    public Properties reportingStreamConfig() {

        String streamId = kafkaStreamsConfig.getApplicationId() + "-reporting-stream";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, getTrustedPackages());

        Map<String, String> additionalProperties = kafkaStreamsConfig.getProperties();
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, additionalProperties.getOrDefault(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000"));
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, additionalProperties.getOrDefault(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1"));
        props.put(StreamsConfig.STATE_DIR_CONFIG, additionalProperties.getOrDefault(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams") + "/" + streamId);

        return props;
    }

    @Bean(name = "persistingStreamConfig")
    public Properties persistingStreamConfig() {

        String streamId = kafkaStreamsConfig.getApplicationId() + "-persisting-stream";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, getTrustedPackages());

        Map<String, String> additionalProperties = kafkaStreamsConfig.getProperties();
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, additionalProperties.getOrDefault(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000"));
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, additionalProperties.getOrDefault(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1"));
        props.put(StreamsConfig.STATE_DIR_CONFIG, additionalProperties.getOrDefault(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams") + "/" + streamId);

        return props;
    }

    private static String getTrustedPackages() {
        return "com.jvo.viewlogreporting.dto.*, com.jvo.viewlogkafkaproducer.dto.ViewLogDto, com.jvo.viewlogkafkaproducer.dto.*";
    }
}
