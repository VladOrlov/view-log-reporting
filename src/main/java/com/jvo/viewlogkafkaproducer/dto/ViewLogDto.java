package com.jvo.viewlogkafkaproducer.dto;

import java.io.Serializable;
import java.time.LocalDateTime;


public record ViewLogDto(String viewId,
                         LocalDateTime startTimestamp,
                         LocalDateTime endTimestamp,
                         long bannerId,
                         Integer campaignId) implements Serializable {

}
