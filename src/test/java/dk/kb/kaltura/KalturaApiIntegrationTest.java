package dk.kb.kaltura;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kaltura.client.types.*;
import dk.kb.kaltura.config.ServiceConfig;
import dk.kb.util.yaml.YAML;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.client.enums.MediaType;

import dk.kb.kaltura.client.DsKalturaClient;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Unittest that will call the API search method. Search for a local refenceId to get the Kaltura internal id for the record.
 * Using Kaltura client v.19.3.3 there is no longer sporadic errors when calling the API.
 *
 */
@Tag("integration")
public class KalturaApiIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(KalturaApiIntegrationTest.class);

    private static final int DEFAULT_SESSION_DURATION_SECONDS = 86400;
    private static final int DEFAULT_REFRESH_THRESHOLD = 3600;

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
        clientSession.logSessionInfo();
    }

    @Test
    public void testErrorHandling() throws Exception {
        DsKalturaClient clientSession = getClient();
        clientSession.getEntry("NotAEntry");
    }

    @Test
    public void kalturaIDsLookup() throws IOException, APIException {
        Map<String, String> map = getClient().getKalturaIds(
                KNOWN_PAIRS.stream().map(e -> e.get(0)).collect(Collectors.toList()));
        log.debug("kalturaIDsLookup() got {} results from {} IDs", map.size(), KNOWN_PAIRS.size());

        for (List<String> knownPair : KNOWN_PAIRS) {
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

        for (List<String> test : KNOWN_PAIRS) {
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
    public void blockStream() throws IOException, APIException {
        String entry_id = "0_xxxxxx";
        boolean success = getClient().blockStreamByEntryId(entry_id);
        assertTrue(success, "The stream was not blocked. Check that the entry id exists.");
    }

    @Test
    public void testDeleteEntry() throws Exception {
        String not_found_entryId = "0_xxxxxx"; //Change to an existing ID if need to test a successful deletion.
        DsKalturaClient clientSession = getClient();
        boolean success = clientSession.deleteStreamByEntryId(not_found_entryId);
        assertTrue(success); //The record does not exist in Kaltura and can therefor not be deleted.
    }

    /**
     * When uploading a file to Kaltura, remember to delete it from the Kaltura
     *
     */
    @Test
    public void kalturaUpload() throws Exception {
        DsKalturaClient clientSession = getClient();
        String file = "/home/xxxx/Videos/test1.mp4"; // <-- Change to local video file
        String referenceId = "ref_test_1234s";
        MediaType mediaType = MediaType.VIDEO;
        String tag = "DS-KALTURA"; //This tag is use for all upload from DS to Kaltura
        String title = "test2 title from unittest";
        String description = "test2 description from unittest";
        String kalturaId = clientSession.uploadMedia(file, referenceId, mediaType, title, description, tag);
        assertNotNull(kalturaId);
    }

    /**
     * When uploading a file to Kaltura, remember to delete it from the Kaltura
     *
     */
    @Test
    public void kalturaUploadWithFlavorParam() throws Exception {
        DsKalturaClient clientSession = getClient();
        String file = "/home/xxxx/Videos/test.mp4"; // <-- Change to local video file
        String referenceId = "ref_test_1234s";
        MediaType mediaType = MediaType.VIDEO;
        String tag = "DS-KALTURA"; //This tag is use for all upload from DS to Kaltura
        String title = "test3 title from unittest";
        String description = "test3 description from unittest";
        Integer flavorParamId = 3; // <-- Change according to MediaType. 3 for lowQ video and 359 for audio
        String kalturaId = clientSession.uploadMedia(file, referenceId, mediaType, title, description, tag, flavorParamId);
        assertNotNull(kalturaId);
    }


    private DsKalturaClient getClient() throws IOException {
        final YAML conf = ServiceConfig.getConfig().getSubMap("kaltura");
        return new DsKalturaClient(
                conf.getString("url"),
                conf.getString("userId"),
                conf.getInteger("partnerId"),
                conf.getString("token"),
                conf.getString("tokenId"),
                conf.getString("adminSecret", null),
                conf.getInteger("sessionDurationSeconds", DEFAULT_SESSION_DURATION_SECONDS),
                conf.getInteger("sessionRefreshThreshold", DEFAULT_REFRESH_THRESHOLD));
    }

}
