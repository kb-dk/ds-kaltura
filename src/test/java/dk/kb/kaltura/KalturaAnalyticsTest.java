package dk.kb.kaltura;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.kaltura.client.enums.*;
import com.kaltura.client.services.MediaService;
import com.kaltura.client.types.*;
import dk.kb.kaltura.client.DsKalturaAnalytics;
import dk.kb.kaltura.config.ServiceConfig;
import dk.kb.util.yaml.YAML;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
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
    public void countAllBaseEntriesTest() throws IOException, APIException {
        DsKalturaAnalytics client = getClient();
        BaseEntryFilter filter = new BaseEntryFilter();
        filter.statusIn(EntryStatus.READY.getValue());
        filter.setModerationStatusNotEqual(EntryModerationStatus.REJECTED);
        int i = client.countAllBaseEntries(filter);
        System.out.println("Total: " + i);
    }

    @Test
    public void countAllMediaEntriesTest() throws IOException, APIException {
//        DsKalturaClient client = getClientPROD();
        DsKalturaAnalytics client = getClient();
        MediaEntryFilter filter = new MediaEntryFilter();
//        filter.statusIn(EntryStatus.READY.getValue());
        filter.setModerationStatusEqual(EntryModerationStatus.REJECTED);
        filter.setCreatedAtGreaterThanOrEqual(1704067200L);
        int i = client.countAllMediaEntries(filter);
        System.out.println("Total: " + i);
    }

    @Test
    public void listAllRejectedEntriesGenericTest() throws Exception {
        DsKalturaAnalytics client = getClient();
        MediaEntryFilter mediaEntryFilter = new MediaEntryFilter();
        mediaEntryFilter.setModerationStatusEqual(EntryModerationStatus.REJECTED);
        String filename = "RejectedEntries_test.json";
        client.exportAllEntriesToFile(mediaEntryFilter, MediaService::list, filename);
    }

    @Test
    public void listAllReadyEntriesGenericTest() throws Exception {
        DsKalturaAnalytics client = getClient();
        MediaEntryFilter mediaEntryFilter = new MediaEntryFilter();
        mediaEntryFilter.statusNotIn("notAStatus");
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
        String filename = "AllEntriesProd2.json";
        client.exportAllEntriesToFile(mediaEntryFilter, MediaService::list,
                filename);
    }


    private void writeToFile(String filename, Set<? extends BaseEntry> rejectedMediaEntries) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (BaseEntry mediaEntry : rejectedMediaEntries) {
                String json = new Gson().toJson(mediaEntry);
//                String json = mediaEntry.toParams().toString();
                writer.write(json);
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            //TODO handle exception
            e.printStackTrace();
        }
    }

    private void writeToFile(String filename, Map<String, String> map) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            writer.write(map.get("header"));
            writer.newLine();
            Arrays.stream(map.get("data").split(";")).forEach(line -> {
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


    public List<MediaEntry> readFromFile(String filename) throws IOException {
        List<MediaEntry> mediaEntries = new ArrayList<>();
        Gson gson = new Gson();

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Parse the JSON line into Params
                JsonObject jsonObject = gson.fromJson(line, JsonObject.class);

                jsonObject.addProperty("moderationStatus", EntryModerationStatus.valueOf(jsonObject.get(
                        "moderationStatus").getAsString()).getValue());

                jsonObject.addProperty("licenseType", LicenseType.valueOf(jsonObject.get(
                        "licenseType").getAsString()).getValue());

                jsonObject.addProperty("displayInSearch", EntryDisplayInSearchType.valueOf(jsonObject.get(
                        "displayInSearch").getAsString()).getValue());

                jsonObject.addProperty("mediaType", MediaType.valueOf(jsonObject.get(
                        "mediaType").getAsString()).getValue());

                mediaEntries.add(new MediaEntry(jsonObject));
            }
        } catch (APIException e) {
            throw new RuntimeException(e);
        }

        return mediaEntries;
    }

    @Test
    public void reportFromJson() throws IOException, APIException {
        YAML conf = ServiceConfig.getConfig().getSubMap("kaltura");
        String fromDay = "20250101";
        String toDay = "20261231";
        String domain = "www.kb.dk";
        String outputFilePath =
                "src/test/resources/test_files/TOP_CONTENT-" + fromDay + "-" + toDay + "-" + conf.getString("partnerId") +
                        "-" + LocalDate.now();
        String inputFilePath = "/home/adpe/IdeaProjects/ds-parent/ds-kaltura/AllEntriesProd1.json";

        ReportInputFilter filter = new ReportInputFilter();
        filter.setDomainIn(domain);
        filter.setFromDay(fromDay);
        filter.setToDay(toDay);

        DsKalturaAnalytics client = getClient();
        client.reportFromJson(inputFilePath, outputFilePath, filter);

    }

    @Test
    public void getReportFromEntryIds() throws Exception {
        DsKalturaAnalytics client = getClient();
        final YAML conf = ServiceConfig.getConfig().getSubMap("kaltura");

        Stream<String> ids =
                readFromFile("/home/adpe/IdeaProjects/ds-parent/ds-kaltura/AllEntriesProd1.json")
                        .stream()
                        .map(x -> x.getId());
        String fromDay = "20250101";
        String toDay = "20261231";
        String domain = "www.kb.dk";

        ReportInputFilter filter = new ReportInputFilter();
        filter.setDomainIn(domain);
        filter.setFromDay(fromDay);
        filter.setToDay(toDay);

        String path =
                "src/test/resources/test_files/TOP_CONTENT-" + fromDay + "-" + toDay + "-" + conf.getString("partnerId") +
                        "-" + LocalDate.now();
        FileWriter fw = new FileWriter(path);

        client.reportFromIds(ids, fw, filter);

    }

    @Test
    public void getReportTableNoObjects() throws Exception {
        DsKalturaAnalytics client = getClient();
        var filter = new ReportInputFilter();
        filter.setFromDay("20250101");
        filter.setToDay("20260101");
        var reportType = ReportType.TOP_CONTENT;
        var map = client.getReportTable(reportType, filter, null, null);
        String path = "src/test/resources/test_files/" + reportType.name() + "-" + LocalDateTime.now();
        writeToFile(path, map);
    }


    private boolean filterReportType(ReportType reportType) {
        switch (reportType) {
            case QUIZ:
            case TOP_PLAYBACK_CONTEXT_VPAAS:
            case QOE_VOD_SESSION_FLOW:
                return true;
            default:
                return false;
        }
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
