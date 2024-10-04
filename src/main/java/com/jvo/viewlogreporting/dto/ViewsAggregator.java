package com.jvo.viewlogreporting.dto;

import lombok.Getter;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Getter
public class ViewsAggregator {

    private int campaignId;
    private int networkId;
    private AtomicLong totalDuration = new AtomicLong(0);
    private AtomicInteger totalCount = new AtomicInteger(0);
    private LocalDateTime windowStartTime;

    public ViewsAggregator add(EnrichedViewLogDto enrichedViewLog) {
        this.campaignId = enrichedViewLog.campaignId();
        this.networkId = enrichedViewLog.networkId();

        this.totalDuration.addAndGet(getViewDurationInSeconds(enrichedViewLog));

        this.totalCount.incrementAndGet();

        if (this.windowStartTime == null) {
            this.windowStartTime = enrichedViewLog.startTimestamp().truncatedTo(ChronoUnit.MINUTES);
        }
        return this;
    }

    private static long getViewDurationInSeconds(EnrichedViewLogDto enrichedViewLog) {
        return Duration.between(enrichedViewLog.startTimestamp(), enrichedViewLog.endTimestamp()).getSeconds();
    }

    public double getAvgDuration() {

        int count = totalCount.get();
        if (count == 0) {
            return 0.0;
        }
        long totalDurationInSeconds = totalDuration.get();
        double avgDuration = (double) totalDurationInSeconds / count;
        return Math.round(avgDuration * 100.0) / 100.0;
    }

}

