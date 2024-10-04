package com.jvo.viewlogreporting.service;

import com.jvo.viewlogreporting.dto.CampaignDataDto;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.StreamSupport;

@Getter
@Slf4j
@Service
public class CampaignDataService {

    private List<CampaignDataDto> campaignData;
    private final String csvFilePath;
    private final Random random;

    public CampaignDataService(@Value("${campaign.data.csv.path}") String csvFilePath) {
        this.csvFilePath = csvFilePath;
        this.random = new Random();
    }


    @PostConstruct
    public void init() {
        if (csvFilePath == null || csvFilePath.isBlank()) {
            log.error("CSV file path is not configured.");
            throw new IllegalArgumentException("Campaign CSV file path is missing.");
        }

        Resource resource = new ClassPathResource(csvFilePath);

        try (Reader reader = new InputStreamReader(resource.getInputStream());
             CSVParser csvParser = getCsvParser(reader)) {

            campaignData = getCampaignData(csvParser);

            if (campaignDataIsEmpty()) {
                log.warn("Campaign data is empty, cannot load campaigns.");
            } else {
                log.info("Successfully load campaign data: {}", campaignData);
            }
        } catch (IOException ex) {
            log.error("Failed to read campaign data!", ex);
        }
    }

    private List<CampaignDataDto> getCampaignData(CSVParser csvParser) {
        return StreamSupport.stream(csvParser.spliterator(), false)
                .parallel()
                .map(this::mapToCampaignDataDto)
                .flatMap(Optional::stream)
                .toList();
    }

    private static CSVParser getCsvParser(Reader reader) throws IOException {
        return new CSVParser(reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withIgnoreHeaderCase()
                .withTrim());
    }

    private Optional<CampaignDataDto> mapToCampaignDataDto(CSVRecord csvRecord) {
        try {
            int networkId = Integer.parseInt(csvRecord.get("network_id"));
            int campaignId = Integer.parseInt(csvRecord.get("campaign_id"));
            String campaignName = csvRecord.get("campaign_name");

            CampaignDataDto campaignDataDto = new CampaignDataDto(networkId, campaignId, campaignName);
            return Optional.of(campaignDataDto);
        } catch (NumberFormatException ex) {
            log.error("Error parsing numeric field in record {}: {}", csvRecord.getRecordNumber(), ex.getMessage());
            return Optional.empty();
        } catch (IllegalArgumentException ex) {
            log.error("Missing or invalid data in record {}: {}", csvRecord.getRecordNumber(), ex.getMessage());
            return Optional.empty();
        }
    }

    private boolean campaignDataIsEmpty() {
        return campaignData == null || campaignData.isEmpty();
    }
}
