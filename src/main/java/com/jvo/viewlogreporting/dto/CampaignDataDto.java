package com.jvo.viewlogreporting.dto;

import java.io.Serializable;

public record CampaignDataDto(Integer networkId,
                              Integer campaignId,
                              String campaignName) implements Serializable {
}
