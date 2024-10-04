package com.jvo.viewlogreporting.service;


import com.jvo.viewlogkafkaproducer.dto.ViewLogDto;
import com.jvo.viewlogreporting.dto.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ViewLogsEnricher implements EventsEnrichStreamService {

    private final SerdeConfig serdeConfig;
    public final Properties enrichingStreamConfig;
    private final String viewLogTopic;
    private final String campaignTopic;
    private final String enrichedViewLogsTopic;

    public ViewLogsEnricher(SerdeConfig serdeConfig,
                            @Qualifier("enrichingStreamConfig") Properties enrichingStreamConfig,
                            @Value("${spring.kafka.topics.view-logs-topic}") String viewLogTopic,
                            @Value("${spring.kafka.topics.campaign-data-topic}") String campaignTopic,
                            @Value("${spring.kafka.topics.enriched-view-logs}") String enrichedViewLogsTopic) {
        this.serdeConfig = serdeConfig;
        this.enrichingStreamConfig = enrichingStreamConfig;
        this.viewLogTopic = viewLogTopic;
        this.campaignTopic = campaignTopic;
        this.enrichedViewLogsTopic = enrichedViewLogsTopic;
    }


    @Override
    public void runEnrichingStream() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        GlobalKTable<String, CampaignDataDto> campaignTable = buildCampaignTable(streamsBuilder);
        KStream<String, ViewLogDto> viewLogsStreamer = buildViewLogsStream(streamsBuilder);

        KStream<String, EnrichedViewLogDto> enrichedViewLogs = viewLogsStreamer
                .join(
                        campaignTable,
                        (viewLogKey, viewLog) -> viewLogKey,
                        ViewLogsEnricher::getEnrichedViewLogDto
                );

        enrichedViewLogs.to(enrichedViewLogsTopic, Produced.with(Serdes.String(), serdeConfig.enrichedViewLogSerde()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), enrichingStreamConfig);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

    private GlobalKTable<String, CampaignDataDto> buildCampaignTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.globalTable(
                campaignTopic,
                Materialized.with(Serdes.String(), serdeConfig.campaignDataSerde()));
    }

    private KStream<String, ViewLogDto> buildViewLogsStream(StreamsBuilder streamsBuilder) {
        return streamsBuilder.stream(
                viewLogTopic,
                Consumed.with(Serdes.String(), serdeConfig.viewLogSerde())
        );
    }

    private static EnrichedViewLogDto getEnrichedViewLogDto(ViewLogDto viewLog, CampaignDataDto campaignDataDto) {
        return new EnrichedViewLogDto(
                viewLog.viewId(),
                viewLog.startTimestamp(),
                viewLog.endTimestamp(),
                viewLog.bannerId(),
                viewLog.campaignId(),
                campaignDataDto.networkId()
        );
    }


    private static ViewsReportDto getViewsReportDto(ViewsAggregator aggregator) {
        return new ViewsReportDto(
                aggregator.getCampaignId(),
                aggregator.getNetworkId(),
                aggregator.getWindowStartTime(),
                aggregator.getAvgDuration(),
                aggregator.getTotalCount().get()
        );
    }

}

