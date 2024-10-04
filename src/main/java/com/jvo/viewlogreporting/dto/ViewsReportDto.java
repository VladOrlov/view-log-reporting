package com.jvo.viewlogreporting.dto;

import java.io.Serializable;
import java.time.LocalDateTime;


public record ViewsReportDto(int campaignId,
                             int networkId,
                             LocalDateTime minuteTimestamp,
                             double avgDuration,
                             int viewCount) implements Serializable {

}
