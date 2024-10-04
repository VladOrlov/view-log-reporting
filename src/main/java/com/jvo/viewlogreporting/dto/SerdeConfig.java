package com.jvo.viewlogreporting.dto;

import com.jvo.viewlogkafkaproducer.dto.ViewLogDto;
import org.springframework.kafka.support.serializer.JsonSerde;

public record SerdeConfig(
        JsonSerde<ViewLogDto> viewLogSerde,
        JsonSerde<CampaignDataDto> campaignDataSerde,
        JsonSerde<EnrichedViewLogDto> enrichedViewLogSerde,
        JsonSerde<ViewsReportDto> viewsReportSerde) {
}