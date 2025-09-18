package dk.kb.kaltura;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kaltura.client.enums.EntryModerationStatus;
import com.kaltura.client.enums.EntryStatus;
import com.kaltura.client.enums.ReportType;
import com.kaltura.client.services.MediaService;
import com.kaltura.client.types.APIException;
import com.kaltura.client.types.BaseEntryFilter;
import com.kaltura.client.types.MediaEntryFilter;
import com.kaltura.client.types.ReportInputFilter;
import dk.kb.kaltura.domain.ReportTableDto;
import dk.kb.kaltura.client.DsKalturaAnalytics;
import dk.kb.kaltura.config.ServiceConfig;
import dk.kb.util.yaml.YAML;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Tag("integration")
public class KalturaAnalyticsTest {

    private static final Logger log = LoggerFactory.getLogger(KalturaAnalyticsTest.class);
    private static final int DEFAULT_SESSION_DURATION_SECONDS = 86400;
    private static final int DEFAULT_REFRESH_THRESHOLD = 3600;


    @BeforeAll
    public static void setup() throws IOException {
        ServiceConfig.initialize("src/main/conf/ds-kaltura-*.yaml"); // Does not seem like a solid construction
        if ("yyyyy".equals(ServiceConfig.getConfig().getString("kaltura.tokenId")) &&
                ("yyyyy".equals(ServiceConfig.getConfig().getString("kaltura.token")))) {
            throw new IllegalStateException("An kaltura.token and kaltura.tokenId must be set to perform integration test. Please generate an appToken and" +
                    "add it to the local configuration (NOT the *-behaviour.YAML configuration)");
        }
    }

    @Test
    public void countAllBaseEntriesTest() throws APIException {
        DsKalturaAnalytics client = getClient();
        BaseEntryFilter filter = new BaseEntryFilter();
        filter.statusIn(EntryStatus.READY.getValue());
        filter.setModerationStatusNotIn("notAStatus");
        int i = client.countAllBaseEntries(filter);
        System.out.println("Total: " + i);
    }

    @Test
    public void countAllMediaEntriesTest() throws APIException {
        DsKalturaAnalytics client = getClient();
        MediaEntryFilter filter = new MediaEntryFilter();
        filter.statusNotIn("notAStatus");
        filter.setModerationStatusNotIn("notAStatus");
        filter.setCreatedAtGreaterThanOrEqual(1704067200L);
        int i = client.countAllMediaEntries(filter);
        System.out.println("Total: " + i);
    }

    @Test
    public void listAllRejectedEntriesGenericTest() throws APIException {
        DsKalturaAnalytics client = getClient();
        MediaEntryFilter mediaEntryFilter = new MediaEntryFilter();
        mediaEntryFilter.setModerationStatusEqual(EntryModerationStatus.REJECTED);
        String filename = "RejectedEntries_test.json";
        client.exportAllEntriesToFile(mediaEntryFilter, MediaService::list, filename);
    }

    @Test
    public void listAllReadyEntriesGenericTest() throws APIException {
        DsKalturaAnalytics client = getClient();
        MediaEntryFilter mediaEntryFilter = new MediaEntryFilter();
        mediaEntryFilter.statusEqual(EntryStatus.READY.getValue());
        mediaEntryFilter.moderationStatusNotIn("notAStatus");
        String filename = "ReadyEntries.json";
        client.exportAllEntriesToFile(mediaEntryFilter, MediaService::list, filename);

    }

    @Test
    public void listAllEntriesGenericTest() throws Exception {
        DsKalturaAnalytics client = getClient();
        MediaEntryFilter mediaEntryFilter = new MediaEntryFilter();
        mediaEntryFilter.statusNotIn("notAStatus");
        mediaEntryFilter.moderationStatusNotIn("notAStatus");
        String filename = "JsonObjects";
        client.exportAllEntriesToFile(mediaEntryFilter, MediaService::list,
                filename);
    }

    @Test
    public void getReportTableNoObjectIds() throws Exception {
        DsKalturaAnalytics client = getClient();
        var filter = new ReportInputFilter();
        filter.setFromDay("20250101");
        filter.setToDay("20260101");
        var reportType = ReportType.TOP_CONTENT;
        var map = client.getReportTable(reportType, filter, null, null);
        String path = "src/test/resources/test_files/" + reportType.name() + "-" + LocalDateTime.now(ZoneId.systemDefault());
        writeToFile(path, map);
    }

    @Test
    public void getReportFromEntryIds() throws Exception {
        final YAML conf = ServiceConfig.getConfig().getSubMap("kaltura");

        List<String> ids =
                readFromFile("./JsonObjects")
                        .stream()
                        .limit(10000)
                        .map(x -> x.get("id").asText())
                        .collect(Collectors.toList());
//        ids.forEach(System.out::println);
        String fromDay = "20250101";
        String toDay = "20251231";
        String domain = "www.kb.dk";

        DsKalturaAnalytics client = getClient();
        var dtos = client.getTopContentFromIdList(fromDay, toDay, "", ids);
        dtos.forEach(System.out::println);
    }

    private void writeToFile(String filename, ReportTableDto reportTableDto) {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), Charset.defaultCharset()))) {
            writer.write(reportTableDto.getHeader());
            writer.newLine();
            Arrays.stream(reportTableDto.getData().split(";")).forEach(line -> {
                try {
                    writer.write(line);
                    writer.newLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            writer.flush();
        } catch (IOException e) {
            //TODO handle exception
            e.printStackTrace();
        }
    }

    public List<JsonNode> readFromFile(String filename) throws IOException {
        List<JsonNode> jsonNodes = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), Charset.defaultCharset()))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Parse the JSON line into Params
                JsonNode jsonNode = mapper.readValue(line, JsonNode.class);
                jsonNodes.add(jsonNode);
            }
        }

        return jsonNodes;
    }

    private DsKalturaAnalytics getClient() throws APIException {
        final YAML conf = ServiceConfig.getConfig().getSubMap("kaltura");
        return new DsKalturaAnalytics(
                conf.getString("url"),
                conf.getString("userId"),
                conf.getInteger("partnerId"),
                conf.getString("token"),
                conf.getString("tokenId"),
                conf.getString("adminSecret", null),
                conf.getInteger("sessionDurationSeconds"),
                conf.getInteger("sessionRefreshThreshold"));
    }
}
