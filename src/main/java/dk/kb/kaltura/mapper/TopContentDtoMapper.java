package dk.kb.kaltura.mapper;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dk.kb.kaltura.domain.ReportTableDto;
import dk.kb.kaltura.domain.TopContentDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class TopContentDtoMapper {

    private static final Logger log = LoggerFactory.getLogger(TopContentDtoMapper.class);

    /**
     * Converts the data in the provided report table DTO into a list of top content DTOs.
     *
     * @param reportTableDto The {@link ReportTableDto} containing the report data to be processed.
     * @return A list of {@link TopContentDto} objects created from the report data.
     */
    public List<TopContentDto> reportDtoToTopContentList(ReportTableDto reportTableDto) throws IOException {

        List<TopContentDto> topContentDtos = new ArrayList<>();
        if (reportTableDto.getTotalCount() == 0) {
            return topContentDtos;
        }

        CsvMapper csvMapper = new CsvMapper();
        csvMapper.registerModule(new JavaTimeModule());

        CsvSchema schema =
                CsvSchema.emptySchema().withHeader().withColumnSeparator(',').withoutQuoteChar();

        Class<TopContentDto> clazz = TopContentDto.class;
        try {
            return csvMapper.readerFor(clazz)
                    .with(schema)
                    .<TopContentDto>readValues(new StringReader(reportTableDto.getHeader() + System.lineSeparator() +
                            //Titles that contains semicolon is already replaced by whitespace.
                            reportTableDto.getData().replace(";", System.lineSeparator())))
                    .readAll();
        } catch (Exception e) {
            log.error(e.getMessage());
            log.error(reportTableDto.toString());
            throw e;
        }
    }
}
