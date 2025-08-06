package dk.kb.kaltura;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.kaltura.client.enums.EntryModerationStatus;
import com.kaltura.client.enums.EntryStatus;
import com.kaltura.client.enums.ReportType;
import com.kaltura.client.services.MediaService;
import com.kaltura.client.types.*;
import dk.kb.kaltura.client.DsKalturaAnalytics;
import dk.kb.kaltura.client.DsKalturaClient;
import dk.kb.kaltura.config.ServiceConfig;
import dk.kb.util.yaml.YAML;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;


@Tag("integration")
public class KalturaAnalyticsTest {

    private static final Logger log = LoggerFactory.getLogger(KalturaAnalyticsTest.class);
    private static final int DEFAULT_SESSION_DURATION_SECONDS = 86400;
    private static final int DEFAULT_REFRESH_THRESHOLD = 3600;

    public static final String KALTURA_ID1 = "0_954nx5eh";
    public static final String KALTURA_ID2 = "0_sjjppu7s";
    public static final String KALTURA_ID3 = "0_zo7k1tgh";
    public static final String REFERENCE_ID1 = "0b1af131-879a-4286-8637-50f0f4b0705f";
    public static final String REFERENCE_ID2 = "9ee1e45a-60e4-4a9d-a44e-72c089bc924d";
    public static final String REFERENCE_ID3 = "8cd60e55-72a2-482e-9715-67a2f884a285";

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
    public void listAllRejectedEntriesGenericTest() throws Exception{
        DsKalturaAnalytics client = getClient();
        MediaEntryFilter mediaEntryFilter = new MediaEntryFilter();
        mediaEntryFilter.setModerationStatusEqual(EntryModerationStatus.REJECTED);
        mediaEntryFilter.setOrderBy("createdAt_ASC");
        Set<? extends BaseEntry> rejectedMediaEntries =
                client.listAllEntriesGeneric(mediaEntryFilter, (filter, pager) -> MediaService.list(filter, pager));
        System.out.println("Total: " + rejectedMediaEntries.size());
        String filename = "RejectedEntries_test.json";
        writeToFile(filename, rejectedMediaEntries);
    }

    @Test
    public void listAllReadyEntriesGenericTest() throws Exception{
        DsKalturaAnalytics client = getClient();
        MediaEntryFilter mediaEntryFilter = new MediaEntryFilter();
        mediaEntryFilter.statusIn(EntryStatus.READY.getValue());
        mediaEntryFilter.setOrderBy("createdAt_ASC");
        Set<? extends BaseEntry> rejectedMediaEntries =
                client.listAllEntriesGeneric(mediaEntryFilter, (filter, pager) -> MediaService.list(filter, pager));
        System.out.println("Total: " + rejectedMediaEntries.size());
        String filename = "ReadyEntries_Prod.json";
        writeToFile(filename, rejectedMediaEntries);
    }


    private void writeToFile(String filename, Set<? extends BaseEntry> rejectedMediaEntries) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (BaseEntry mediaEntry : rejectedMediaEntries) {
//                String json = new Gson().toJson(mediaEntry);
                String json = mediaEntry.toParams().toString();
                writer.write(json);
                writer.newLine();
            }
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

                // Convert Params to MediaEntry
                MediaEntry mediaEntry = null;
                try {
                    mediaEntry = new MediaEntry(jsonObject);
                } catch (APIException e) {
                    throw new RuntimeException(e);
                }

                mediaEntries.add(mediaEntry);
            }
        }

        return mediaEntries;
    }

    @Test
    public void getReportTableTest() throws Exception{
        DsKalturaAnalytics client = (DsKalturaAnalytics) getClient();
        ReportInputFilter reportInputFilter = new ReportInputFilter();
        reportInputFilter.setFromDay("20250101");
        reportInputFilter.setToDay("20260101");
        reportInputFilter.setTimeZoneOffset(-120);

        StringBuilder objectIdsStringBuilder =  new StringBuilder();

        //prod ids
//        objectIdsStringBuilder.append("0_21qh7wl7").append(",");
//        objectIdsStringBuilder.append("0_b5mfqj2l").append(",");
//        objectIdsStringBuilder.append("0_8tfuroko").append(",");
//        objectIdsStringBuilder.append("0_htitlo23").append(",");
//        objectIdsStringBuilder.append("0_4vl40nrx").append(",");


        for (ReportType reportType : ReportType.values()) {
            if (filterReportType(reportType)) {
                continue;
            }
            System.out.println("Trying with reportType: " + reportType.name());
            try {
                client.getReportTable(reportType, reportInputFilter,
                        null, String.valueOf(objectIdsStringBuilder));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (APIException e) {
                throw new RuntimeException(e);
            }
        }



//        for(List<String> row :rows){
//            System.out.println(row);
//        }

//        List<String> ids = rows.stream().map(row -> row.get(0)).collect(Collectors.toList());
//        Map<String, Integer> countMap = new HashMap<>();
//        List<String> duplicates = new ArrayList<>();
//
//        for (String line : ids) {
//            countMap.put(line, countMap.getOrDefault(line, 0) + 1);
//        }
//
//        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
//            if (entry.getValue() > 1) {
//                duplicates.add(entry.getKey());
//            }
//        }
//        log.debug("Duplicates count = {}", duplicates.size());
//        log.debug("{}", ids.size());


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

    //Tries to use createdAt to segment data. However this does not work as intended since more than 10000 entires
    // exists on some days and the createdAt is truncated to days in analytics.
    @Test
    public void getReportALLTableTest() throws Exception{
        DsKalturaAnalytics client = (DsKalturaAnalytics) getClient();
        ReportInputFilter reportInputFilter = new ReportInputFilter();
        reportInputFilter.setFromDay("20250101");
        reportInputFilter.setToDay("20260101");
        reportInputFilter.setDomainIn("www.kb.dk");

        List<String> segments = new ArrayList<>();
        try(BufferedReader br = new BufferedReader(new FileReader("/home/adpe/IdeaProjects/ds-kaltura/src/test" +
                "/resources" +
                "/test_files" +
                "/createdAt_segments"))) {

            String line = br.readLine();

            while (line != null) {
                segments.add(line);
                line = br.readLine();
            }
        }

        Map<String, List<List<String>>> segmentsMap = client.getReportTableFromSegments(segments, reportInputFilter);

        for(String key : segmentsMap.keySet()){
            List<List<String>> rows = segmentsMap.get(key);
            try (BufferedWriter writer =
                         new BufferedWriter(new FileWriter("./src/test/resources/test_files/SegmentedData/"+ key))) {
                for (List<String> sublist : segmentsMap.get(key)) {
                    // Join the sublist strings with commas
                    String line = String.join(",", sublist);
                    // Write the line to the file
                    writer.write(line);
                    writer.newLine(); // Move to the next line
                }
                // Detect duplicates
                List<String> ids = rows.stream().map(row -> row.get(0)).collect(Collectors.toList());
                Map<String, Integer> countMap = new HashMap<>();
                List<String> duplicates = new ArrayList<>();

                for (String line : ids) {
                    countMap.put(line, countMap.getOrDefault(line, 0) + 1);
                }

                for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
                    if (entry.getValue() > 1) {
                        duplicates.add(entry.getKey());
                    }
                }

                if(!duplicates.isEmpty()) {
                    log.debug("{} contains duplicate entry ids: {}",key,
                            countMap.entrySet().stream().filter( entry -> entry.getValue() > 1).collect(Collectors.toList()));
                }
                System.out.println("Data written to file successfully.");
            } catch (IOException e) {
                System.err.println("An error occurred while writing to the file: " + e.getMessage());
            }
        }
    }


    // Error reports is deemed surplus to requirements
    @Test
    public void createErrorReport() throws Exception{
        DsKalturaAnalytics client =  (DsKalturaAnalytics) getClient();
        ReportInputFilter reportInputFilter = new ReportInputFilter();
        reportInputFilter.setFromDay("20250330");
        reportInputFilter.setToDay("20990101");
        reportInputFilter.setDomainIn("www.kb.dk");

        List<List<String>> rows = client.getReportTable(ReportType.QOE_ERROR_TRACKING_CODES, reportInputFilter,
                "");

        Map <String, List<String>> rowMap = rows.stream().collect(Collectors.toMap(x -> x.get(0), x -> x.subList(1, x.size())));

        File error_f = new File("src/test/resources/kaltura-playkit-error-codes");
        final YAML error_yaml =  YAML.parse(error_f).getSubMap("ERRORTYPES");
        // Flip keys and values
        Map<String, String> flippedData = new HashMap<>();
        for (Map.Entry<String, Object> entry : error_yaml.entrySet()) {
            flippedData.put(entry.getValue().toString(), entry.getKey());
        }

        for (String errorCode : rowMap.keySet()){
            if(errorCode.equals("errorcode")) {
                continue;
            }
            else if (errorCode.equals("Unknown")) {
                System.out.println((flippedData.get(errorCode) != null ? flippedData.get(errorCode) :
                        "UNKNOWN") + ":" + errorCode + " | count: " + rowMap.get(errorCode).get(1));
                System.out.println("\n-------------------------------------\n");
                continue;
            }

            reportInputFilter.errorCodeIn(errorCode.toString());

            List<List<String>> rows_loop = client.getReportTable(ReportType.QOE_ERROR_TRACKING_BROWSERS,
                    reportInputFilter, "error");

            if (rows_loop.size() != 0) {
                System.out.println((flippedData.get(errorCode) != null ? flippedData.get(errorCode) :
                        "UNKNOWN") + ":" + errorCode + " | count: " + rowMap.get(errorCode).get(1));
                for (List<String> i : rows_loop) {
                    System.out.println(i.toString());
                }
                System.out.println("\n-------------------------------------\n");
            }
        }
    }


    //Only return the top xx results, NOT a full report
    @Test
    public void getCsvUrl() throws APIException, IOException {

        DsKalturaAnalytics clientSesison = getClient();
        ReportInputFilter filter = new ReportInputFilter();
        filter.setFromDay("20250501");
        filter.setToDay("20250601");
        ReportResponseOptions  responseOptions = new ReportResponseOptions();
        responseOptions.delimiter(",");
        responseOptions.setSkipEmptyDates(true);

        String url = clientSesison.getUrlForReportAsCsv(
                "Top vidoes",
                "Some text here",
                "1,2,3,4,5,6,7,8,9;1,2,3,4,5,6,7,8,9",
                ReportType.TOP_CONTENT,
                filter,
                "500x500",null,null,null, responseOptions);

        System.out.println("URL: " + url);

    }

    // Uses the same export method as KMC, an therefore fails with larger data sets
    @Test
    public void exportCsv() throws APIException, IOException {
        DsKalturaAnalytics client = getClient();

        ReportInputFilter filter = new ReportInputFilter();
        filter.setFromDay("20250101");
        filter.setToDay("20260101");
        filter.setDomainIn("www.kb.dk");

        client.exportTopContent(filter, "kesj@kb.dk");
    }

    private DsKalturaAnalytics getClient() throws IOException {
        final YAML conf = ServiceConfig.getConfig().getSubMap("kaltura");
        return new DsKalturaAnalytics(
                conf.getString("url"),
                conf.getString("userId"),
                conf.getInteger("partnerId"),
                conf.getString("token"),
                conf.getString("tokenId"),
                conf.getString("adminSecret",null),
                conf.getInteger("sessionDurationSeconds"),
                conf.getInteger("sessionRefreshThreshold"));
    }
}
