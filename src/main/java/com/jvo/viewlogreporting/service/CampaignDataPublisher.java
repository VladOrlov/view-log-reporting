package com.jvo.viewlogreporting.service;

import com.jvo.viewlogreporting.dto.CampaignDataDto;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;


import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class CampaignDataPublisher {

    private final String topicName;
    private final KafkaTemplate<String, CampaignDataDto> kafkaTemplate;
    private final CampaignDataService campaignDataService;


    public CampaignDataPublisher(KafkaTemplate<String, CampaignDataDto> kafkaTemplate,
                                 CampaignDataService campaignDataService,
                                 @Value("${spring.kafka.topics.campaign-data-topic}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.campaignDataService = campaignDataService;
        this.topicName = topicName;
    }

    @PostConstruct
    public void publishCampaign() {
        List<CampaignDataDto> campaignData = campaignDataService.getCampaignData();
        campaignData.forEach(this::publishToKafkaTopic);
        kafkaTemplate.flush();
    }

    private CompletableFuture<Void> publishToKafkaTopic(CampaignDataDto campaignDataDto) {
        return kafkaTemplate.send(topicName, String.valueOf(campaignDataDto.campaignId()), campaignDataDto)
                .thenAcceptAsync(this::logResult);
    }

    private void logResult(SendResult<String, CampaignDataDto> sendResult) {
        log.info("Publishing message to topic {}. Result: {}", topicName, sendResult);
    }

}
