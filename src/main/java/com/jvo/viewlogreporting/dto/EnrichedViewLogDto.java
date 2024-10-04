package com.jvo.viewlogreporting.dto;

import java.io.Serializable;
import java.time.LocalDateTime;


public record EnrichedViewLogDto(String viewId,
                                 LocalDateTime startTimestamp,
                                 LocalDateTime endTimestamp,
                                 long bannerId,
                                 int campaignId,
                                 int networkId) implements Serializable {

}
