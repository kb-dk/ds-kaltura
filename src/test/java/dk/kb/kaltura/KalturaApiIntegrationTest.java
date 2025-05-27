package dk.kb.kaltura;

import java.io.*;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.stream.Collectors;

import com.kaltura.client.enums.ReportOrderBy;
import com.kaltura.client.enums.ReportType;
import com.kaltura.client.types.APIException;
import com.kaltura.client.types.AppToken;
import com.kaltura.client.types.ESearchEntryOperator;
import com.kaltura.client.types.ReportInputFilter;
import dk.kb.kaltura.client.AppTokenClient;
import dk.kb.kaltura.config.ServiceConfig;
import dk.kb.util.DatetimeParser;
import dk.kb.util.yaml.YAML;
import okio.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.client.enums.MediaType;

import dk.kb.kaltura.client.DsKalturaClient;

import javax.print.attribute.standard.DateTimeAtCreation;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Unittest that will call the API search method. Search for a local refenceId to get the Kaltura internal id for the record.
 * Using Kaltura client v.19.3.3 there is no longer sporadic errors when calling the API.
 *
 */
@Tag("integration")
public class KalturaApiIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(KalturaApiIntegrationTest.class);

    private static final long DEFAULT_KEEP_ALIVE = 86400;

    // ID's valid as of 2024-04-25 but subject to change
    // TODO: Add a step to setup() creating test kaltura<->reference IDs
    public static final String KALTURA_ID1 = "0_954nx5eh";
    public static final String KALTURA_ID2 = "0_sjjppu7s";
    public static final String KALTURA_ID3 = "0_zo7k1tgh";
    public static final String REFERENCE_ID1 = "0b1af131-879a-4286-8637-50f0f4b0705f";
    public static final String REFERENCE_ID2 = "9ee1e45a-60e4-4a9d-a44e-72c089bc924d";
    public static final String REFERENCE_ID3 = "8cd60e55-72a2-482e-9715-67a2f884a285";

    // referenceID, kalturaID
    public static final List<List<String>> KNOWN_PAIRS_1 = List.of(
            List.of(REFERENCE_ID1, KALTURA_ID1)
    );
    public static final List<List<String>> KNOWN_PAIRS_3 = List.of(
            List.of(REFERENCE_ID1, KALTURA_ID1),
            List.of(REFERENCE_ID2, KALTURA_ID2),
            List.of(REFERENCE_ID3, KALTURA_ID3)
    );
    public static final List<List<String>> KNOWN_PAIRS_OLD = List.of(
            List.of("7f7ffcbc-58dc-40bd-8ca9-12d0f9cf3ed7", "0_vvp1ozjl")
    );

    // The IDs used by the unit test
    public static final List<List<String>> KNOWN_PAIRS = KNOWN_PAIRS_3;

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
    public void testKalturaSession() throws Exception {
        DsKalturaClient clientSession = getClient();
    }

    @Test
    public void kalturaIDsLookup() throws IOException, APIException {
        Map<String, String> map = getClient().getKalturaIds(
                KNOWN_PAIRS.stream().map(e -> e.get(0)).collect(Collectors.toList()));
        log.debug("kalturaIDsLookup() got {} results from {} IDs", map.size(), KNOWN_PAIRS.size());

        for (List<String> knownPair: KNOWN_PAIRS) {
            String refID = knownPair.get(0);
            String kalID = knownPair.get(1);
            assertTrue(map.containsKey(refID), "There should be a mapping for referenceId '" + refID + "'");
            assertEquals(kalID, map.get(refID), "The mapping for '" + refID + " should be as expected");
        }
    }

    // We have no scenario where this lookup is used
    @Test
    public void referenceIDsLookup() throws IOException, APIException {
        Map<String, String> map = getClient().getReferenceIds(
                KNOWN_PAIRS.stream().map(e -> e.get(1)).collect(Collectors.toList()));
        log.debug("referenceIDsLookup() got {} hits for {} kalturaIDs", map.size(), KNOWN_PAIRS.size());

        for (List<String> test: KNOWN_PAIRS) {
            String refID = test.get(0);
            String kalID = test.get(1);
            assertTrue(map.containsKey(kalID), "There should be a mapping for kalturaId '" + kalID + "'");
            assertEquals(refID, map.get(kalID), "The mapping for '" + kalID + " should be as expected");
        }
    }

    @Test
    public void simpleSearch() throws IOException, APIException {
        List<String> ids = getClient().searchTerm("dr");
        assertFalse(ids.isEmpty(), "Search result should not be empty");
        System.out.println(ids);
    }


    @Test
    public void blockStream() throws IOException {
        String entry_id="0_h5p9kkqk"; // Hvornår var det nu det var  (tv, stage miljø)

        boolean  success = getClient().blockStreamByEntryId(entry_id);
        assertTrue(success,"The steam was not blocked.Check that the entry id exists.");
    }


    // Old stress test to see why repeated calls failed (they don't anymore)
    @Disabled
    public void callKalturaApi() throws Exception{

        //These data can change in Kaltura
        String referenceId="7f7ffcbc-58dc-40bd-8ca9-12d0f9cf3ed7";
        String kalturaInternallId="0_vvp1ozjl";

        DsKalturaClient clientSession= getClient();

        int success=0;
        for (int i = 0;i<10;i++) {
            String kalturaId = clientSession.getKalturaInternalId(referenceId);
            assertEquals(kalturaInternallId, kalturaId,"API error was reproduced after "+success+" number of calls");
            log.debug("API returned internal Kaltura id:"+kalturaId);
            success++;
            Thread.sleep(1000L);
        }
    }


    @Test
    public void testDeleteEntry() throws Exception{
        String not_found_entryId="0_xxxxxx"; //Change to an existing ID if need to test a successful deletion.
        DsKalturaClient clientSession= getClient();
        boolean success= clientSession.deleteStreamByEntryId(not_found_entryId);
        assertFalse(success); //The record does not exist in Kaltura and can therefor not be deleted.
    }

    /**
     * When uploading a file to Kaltura, remember to delete it from the Kaltura
     *
     */
    @Test
    public void kalturaUpload() throws Exception{
        DsKalturaClient clientSession= getClient();
        String file="/home/xxxx/Videos/test1.mp4"; // <-- Change to local video file
        String referenceId="ref_test_1234s";
        MediaType mediaType=MediaType.VIDEO;
        String tag="DS-KALTURA"; //This tag is use for all upload from DS to Kaltura
        String title="test2 title from unittest";
        String description="test2 description from unittest";
        String kalturaId = clientSession.uploadMedia(file,referenceId,mediaType,title,description,tag);
        assertNotNull(kalturaId);
    }

    /**
     * When uploading a file to Kaltura, remember to delete it from the Kaltura
     *
     */
    @Test
    public void kalturaUploadWithFlavorParam() throws Exception{
        DsKalturaClient clientSession= getClient();
        String file="/home/xxxx/Videos/test.mp4"; // <-- Change to local video file
        String referenceId="ref_test_1234s";
        MediaType mediaType=MediaType.VIDEO;
        String tag="DS-KALTURA"; //This tag is use for all upload from DS to Kaltura
        String title="test3 title from unittest";
        String description="test3 description from unittest";
        Integer flavorParamId = 3; // <-- Change according to MediaType. 3 for lowQ video and 359 for audio
        String kalturaId = clientSession.uploadMedia(file,referenceId,mediaType,title,description,tag, flavorParamId);
        assertNotNull(kalturaId);
    }

    @Test
    public void getReportTableTest() throws Exception{
        DsKalturaClient client = getClient();
        ReportInputFilter reportInputFilter = new ReportInputFilter();
        reportInputFilter.setFromDay("20230330");
        reportInputFilter.setToDay("20260430");

        List<List<String>> rows = client.getReportTable(ReportType.TOP_CONTENT, reportInputFilter,
                null);

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
        log.debug("Duplicates count = {}", duplicates.size());
//        duplicates.stream().forEach(System.out::println);

        System.out.println(rows);

    }

    @Test
    public void getReportALLTableTest() throws Exception{
        DsKalturaClient client = getClient();
        ReportInputFilter reportInputFilter = new ReportInputFilter();
        reportInputFilter.setFromDay("20240330");
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

        List<List<String>> rows = client.getReportTableFromSegments(segments, reportInputFilter);

        try (BufferedWriter writer =
                     new BufferedWriter(new FileWriter("./src/test/resources/test_files/results-"+ LocalDateTime.now()))) {
            for (List<String> sublist : rows) {
                // Join the sublist strings with commas
                String line = String.join(",", sublist);
                // Write the line to the file
                writer.write(line);
                writer.newLine(); // Move to the next line
            }
            System.out.println("Data written to file successfully.");
        } catch (IOException e) {
            System.err.println("An error occurred while writing to the file: " + e.getMessage());
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
            log.debug("Report contains duplicate entry id: {}", duplicates);
        }
    }



    @Test
    public void createErrorReport() throws Exception{
        DsKalturaClient client = getClient();
        ReportInputFilter reportInputFilter = new ReportInputFilter();
        reportInputFilter.setFromDay("20250330");
        reportInputFilter.setToDay("20990101");

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


    @Test
    public void listAppTokens() throws Exception{
        AppTokenClient client = new AppTokenClient(ServiceConfig.getConfig().getString("kaltura.adminSecret"));
        List<AppToken> tokens = client.listAppTokens();
        tokens.stream().forEach((appToken) -> {
            System.out.println("token:"+appToken.getToken() +" tokenId: "+appToken.getId()+" "+appToken.getCreatedAt()+" "+appToken.getExpiry()+" "+appToken.getSessionUserId()+" "+appToken.getDescription());
        });
    }

    @Test
    public void addAppToken() throws Exception{
        AppTokenClient client = new AppTokenClient(ServiceConfig.getConfig().getString("kaltura.adminSecret"));
        AppToken appToken = client.addAppToken("description");
        System.out.println(appToken.getId()+" "+appToken.getToken()+" "+appToken.getExpiry()+" "+appToken.getDescription());
    }

    @Test
    public void deleteAppToken() throws Exception {
        AppTokenClient client = new AppTokenClient(ServiceConfig.getConfig().getString("kaltura.adminSecret"));
        client.deleteAppToken("0_zjli5ev2");
    }

    private DsKalturaClient getClient() throws IOException {
        final YAML conf = ServiceConfig.getConfig().getSubMap("kaltura");
        return new DsKalturaClient(
                conf.getString("url"),
                conf.getString("userId"),
                conf.getInteger("partnerId"),
                conf.getString("token"),
                conf.getString("tokenId"),
                conf.getString("adminSecret",null),
                conf.getLong("sessionKeepAliveSeconds", DEFAULT_KEEP_ALIVE));
    }

}
