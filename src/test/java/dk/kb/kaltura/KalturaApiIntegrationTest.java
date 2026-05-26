package dk.kb.kaltura;

import com.kaltura.client.enums.MediaType;
import com.kaltura.client.types.APIException;
import dk.kb.kaltura.client.DsKalturaClient;
import dk.kb.kaltura.config.ServiceConfig;
import dk.kb.kaltura.enums.FileExtension;
import dk.kb.util.yaml.YAML;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

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
    public void simpleSearch() throws APIException {
        List<String> ids = getClient().searchTerm("dr");
        assertFalse(ids.isEmpty(), "Search result should not be empty");
    }

    @Test
    public void blockStream() throws APIException {
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
    public void uploadDefault() throws Exception {
        DsKalturaClient clientSession = getClient();
        String file = "/home/xxxx/Videos/test1.mp4"; // <-- Change to local video file
        String referenceId = "ref_test_1234s";
        MediaType mediaType = MediaType.VIDEO;
        String tag = "DS-KALTURA"; //This tag is use for all upload from DS to Kaltura
        String title = "test2 title from unittest";
        String description = "test2 description from unittest";
        FileExtension fileExtension = FileExtension.MP4;
        int conversionProfileId = 0; // <-- change to relevant conversionProfileId found in KMC

        //Upload with default flavor and default conversionProfileID
        String kalturaId = clientSession.uploadMedia(file, referenceId, mediaType, title, description, tag,
                fileExtension, conversionProfileId);
        assertNotNull(kalturaId);
    }

    @Test
    public void uploadMisMatchExt() throws Exception {
        DsKalturaClient clientSession = getClient();
        String file = "/path/to/file"; //<--Change to local video file with mp4
        String referenceId = "ref_test_1234s";
        MediaType mediaType = MediaType.AUDIO;
        String tag = "DS-KALTURA"; //This tag is use for all upload from DS to Kaltura
        String title = "test3 title from unittest";
        String description = "test3 description from unittest";
        int conversionProfileId = 0;

        Throwable t = assertThrows(Exception.class, () -> clientSession.uploadMedia(file, referenceId, mediaType, title,
                description, tag, FileExtension.MP3, conversionProfileId));
        log.debug(t.toString());
    }

    private DsKalturaClient getClient() throws APIException {
        final YAML conf = ServiceConfig.getConfig().getSubMap("kaltura");
        return new DsKalturaClient(
                conf.getString("url"),
                conf.getString("userId"),
                conf.getInteger("partnerId"),
                conf.getString("token"),
                conf.getString("tokenId"),
                conf.getString("adminSecret", null),
                conf.getInteger("sessionDurationSeconds", DEFAULT_SESSION_DURATION_SECONDS),
                conf.getInteger("sessionRefreshThreshold", DEFAULT_REFRESH_THRESHOLD),
                conf.getInteger("conversionQueueThreshold"),
                conf.getInteger("conversionQueueDelaySeconds"));
    }

}
