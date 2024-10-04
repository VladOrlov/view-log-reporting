package com.jvo.viewlogreporting.service;

import com.jvo.viewlogreporting.dto.SerdeConfig;
import com.jvo.viewlogreporting.dto.ViewsReportDto;
import com.jvo.viewlogreporting.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Slf4j
@Service
public class ViewsReportToParquetService implements PersistenceStreamService {

    private final String reportTopic;
    private final String outputBasePath;
    private final Schema schema;
    private final SerdeConfig serdeConfig;
    private final Properties persistingStreamConfig;

    public ViewsReportToParquetService(SerdeConfig serdeConfig,
                                       @Qualifier("persistingStreamConfig") Properties persistingStreamConfig,
                                       @Value("${spring.kafka.topics.views-report}") String reportTopic,
                                       @Value("${output.path}") String outputBasePath) {

        this.serdeConfig = serdeConfig;
        this.persistingStreamConfig = persistingStreamConfig;
        this.reportTopic = reportTopic;
        this.outputBasePath = outputBasePath;
        try {
            schema = new Schema.Parser().parse(getClass().getResourceAsStream("/views_report_schema.avsc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void runPersistenceStream() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, ViewsReportDto> viewsReportStream = streamsBuilder.stream(
                reportTopic,
                Consumed.with(Serdes.String(), serdeConfig.viewsReportSerde())
        );

        KGroupedStream<String, ViewsReportDto> groupedViewsReportStream = viewsReportStream
                .groupBy((key, viewsReportDto) -> getViewReportKey(viewsReportDto),
                        Grouped.with(Serdes.String(), serdeConfig.viewsReportSerde()));

        KTable<String, List<ViewsReportDto>> aggregatedViewReports =
                groupedViewsReportStream
                        .aggregate(
                                ArrayList<ViewsReportDto>::new,
                                (key, viewsReportDto, aggregator) -> {
                                    aggregator.add(viewsReportDto);
                                    return aggregator;
                                },
                                withViewReportListMaterialised()
                        );

        aggregatedViewReports.toStream()
                .foreach((key, viewsReportDto) -> writeToParquet(getFilePath(key), viewsReportDto));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), persistingStreamConfig);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private static String getViewReportKey(ViewsReportDto viewsReportDto) {
        return viewsReportDto.networkId() + "-" + DateUtils.asSlashedString(viewsReportDto.minuteTimestamp());
    }

    private Materialized<String, List<ViewsReportDto>, KeyValueStore<Bytes, byte[]>> withViewReportListMaterialised() {
        return Materialized.with(Serdes.String(), Serdes.ListSerde(ArrayList.class, serdeConfig.viewsReportSerde()));
    }

    private String getFilePath(String reportKey) {
        String[] keyParts = reportKey.split("-");
        String networkId = keyParts[0];
        String minuteTimestamp = keyParts[1];

        String outputDirectory = outputBasePath + networkId + "/" + minuteTimestamp + "/";
        return outputDirectory + UUID.randomUUID().toString() + ".parquet";
    }

    private void writeToParquet(String outputFilePath, List<ViewsReportDto> viewsReportList) {
        Configuration conf = new Configuration();
        ParquetWriter<GenericData.Record> writer = null;

        try {

            Path path = new Path(outputFilePath);
            HadoopOutputFile outputFile = HadoopOutputFile.fromPath(path, conf);

            writer = AvroParquetWriter
                    .<GenericData.Record>builder(outputFile)
                    .withSchema(schema)  // Avro schema for ViewsReportDto
                    .withConf(conf)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build();

            // Write each ViewsReportDto in the list to the Parquet file
            for (ViewsReportDto viewsReportDto : viewsReportList) {
                GenericData.Record record = convertToAvroRecord(viewsReportDto);
                writer.write(record);
            }
        } catch (IOException e) {
            log.error("IOException while writing to Parquet: ", e);
        } catch (RuntimeException e) {
            log.error("RuntimeException while processing the ViewsReportDto list: ", e);
        } catch (Exception e) {
            log.error("Unexpected exception occurred: ", e);
        } finally {
            // Ensure the Parquet writer is closed even if an exception occurs
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    log.error("Failed to close the Parquet writer: ", e);
                }
            }
        }
    }

    private GenericData.Record convertToAvroRecord(ViewsReportDto viewsReportDto) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("networkId", viewsReportDto.networkId());
        record.put("campaignId", viewsReportDto.campaignId());
        record.put("minuteTimestamp", viewsReportDto.minuteTimestamp());
        record.put("viewCount", viewsReportDto.viewCount());
        record.put("avgDuration", viewsReportDto.avgDuration());

        return record;
    }

}
