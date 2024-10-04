package com.jvo.viewlogreporting;

import com.jvo.viewlogreporting.service.ViewLogsEnricher;
import com.jvo.viewlogreporting.service.ViewsReportService;
import com.jvo.viewlogreporting.service.ViewsReportToParquetService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class ViewLogReportingApplication implements CommandLineRunner {

    private final ViewLogsEnricher viewLogsEnricher;
    private final ViewsReportService viewsReportService;
    private final ViewsReportToParquetService viewsReportToParquetService;

    public ViewLogReportingApplication(ViewLogsEnricher viewLogsEnricher, ViewsReportService viewsReportService, ViewsReportToParquetService viewsReportToParquetService) {
        this.viewLogsEnricher = viewLogsEnricher;
        this.viewsReportService = viewsReportService;
        this.viewsReportToParquetService = viewsReportToParquetService;
    }

    public static void main(String[] args) {
        SpringApplication.run(ViewLogReportingApplication.class, args);
    }

    @Override
    public void run(String... args) {
        viewLogsEnricher.runEnrichingStream();
        viewsReportService.runAggregationStream();
        viewsReportToParquetService.runPersistenceStream();
    }
}
