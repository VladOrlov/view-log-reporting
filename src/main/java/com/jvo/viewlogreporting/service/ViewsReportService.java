package com.jvo.viewlogreporting.service;

import com.jvo.viewlogreporting.dto.EnrichedViewLogDto;
import com.jvo.viewlogreporting.dto.SerdeConfig;
import com.jvo.viewlogreporting.dto.ViewsAggregator;
import com.jvo.viewlogreporting.dto.ViewsReportDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Properties;

@Slf4j
@Service
public class ViewsReportService implements AggregationStreamService {

    private final SerdeConfig serdeConfig;
    private final Properties reportingStreamConfig;
    private final String enrichedViewLogsTopic;
    private final String reportTopic;

    public ViewsReportService(SerdeConfig serdeConfig,
                              @Qualifier("reportingStreamConfig") Properties reportingStreamConfig,
                              @Value("${spring.kafka.topics.enriched-view-logs}") String enrichedViewLogsTopic,
                              @Value("${spring.kafka.topics.views-report}") String reportTopic) {

        this.serdeConfig = serdeConfig;
        this.reportingStreamConfig = reportingStreamConfig;
        this.enrichedViewLogsTopic = enrichedViewLogsTopic;
        this.reportTopic = reportTopic;
    }

    public void runAggregationStream() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, EnrichedViewLogDto> enrichedViewLogsStream = streamsBuilder.stream(
                enrichedViewLogsTopic,
                Consumed.with(Serdes.String(), serdeConfig.enrichedViewLogSerde())
        );

        toAggregatedViewsReport(enrichedViewLogsStream);

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), reportingStreamConfig);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }


    private void toAggregatedViewsReport(KStream<String, EnrichedViewLogDto> enrichedViewLogs) {

        TimeWindows tumblingWindow = TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(10));

        KTable<Windowed<String>, ViewsReportDto> viewsReport = enrichedViewLogs
                .groupByKey()
                .windowedBy(tumblingWindow)
                .aggregate(
                        ViewsAggregator::new,
                        (key, enrichedViewLog, aggregator) -> aggregator.add(enrichedViewLog),
                        Materialized.with(Serdes.String(), new JsonSerde<>(ViewsAggregator.class))
                )
                .mapValues(ViewsReportService::getViewsReportDto);

        viewsReport
                .toStream()
                .to(reportTopic, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), serdeConfig.viewsReportSerde()));

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
