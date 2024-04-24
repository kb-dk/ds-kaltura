package dk.kb.kaltura;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    
    private static final long DEFAULT_KEEP_ALIVE = 86400;

    @BeforeAll
    public static void setup() throws IOException {
        ServiceConfig.initialize("src/main/conf/ds-kaltura-*.yaml"); // Does not seem like a solid construction
        if ("xxxxx".equals(ServiceConfig.getConfig().getString("kaltura.adminSecret"))) {
            throw new IllegalStateException("The kaltura.adminSecret must be set to perform integration test. " +
                    "Please add it to the local configuration (NOT the *-behaviour.YAML configuration)");
        }
    }

    @Test
    public void multipleLookups() throws IOException {
        //These data can change in Kaltura
        String referenceId="7f7ffcbc-58dc-40bd-8ca9-12d0f9cf3ed7";
        String kalturaInternallId="0_vvp1ozjl";


        List<List<String>> tests = List.of(
                List.of(referenceId,kalturaInternallId)
        );

        DsKalturaClient clientSession = getClientSession();
        Map<String, String> map = clientSession.getKulturaInternalIds(
                tests.stream().map(e -> e.get(0)).collect(Collectors.toList()));

        for (List<String> test: tests) {
            String refID = test.get(0);
            String kalID = test.get(1);
          assertTrue(map.containsKey(refID), "There should be a mapping for referenceId '" + refID + "'");
          assertEquals(kalID, map.get(refID), "The mapping for '" + refID + " should be as expected");
        }
    }

    @Test
    public void callKalturaApi() throws Exception{
                               
        //These data can change in Kaltura
        String referenceId="7f7ffcbc-58dc-40bd-8ca9-12d0f9cf3ed7"; 
        String kalturaInternallId="0_vvp1ozjl";

        DsKalturaClient clientSession=getClientSession();
        
        int success=0;
        for (int i = 0;i<10;i++) {                  
            String kalturaId = clientSession.getKulturaInternalId(referenceId);
            assertEquals(kalturaInternallId, kalturaId,"API error was reproduced after "+success+" number of calls");
            log.debug("API returned internal Kaltura id:"+kalturaId);
            success++;            
            Thread.sleep(1000L);                        
        }        
    }
    
    
    /**
     * When uploading a file to Kaltura, remember to delete it from the Kaltura 
     * 
     */
    @Test
    public void kalturaUpload() throws Exception{                             
            DsKalturaClient clientSession=getClientSession();                  
            String file="/home/xxx/videos/test.mp4"; // <-- Change to local video file
            String referenceId="ref_test_1234s";
            MediaType mediaType=MediaType.VIDEO;
            String tag="DS-KALTURA"; //This tag is use for all upload from DS to Kaltura
            String title="test2 title from unittest";
            String description="test2 description from unittest";            
            String kalturaId = clientSession.uploadMedia(file, referenceId,mediaType,title,description,tag);   
            assertNotNull(kalturaId);
     }
        
    private DsKalturaClient getClientSession() throws IOException {
        final YAML conf = ServiceConfig.getConfig().getSubMap("kaltura");
        return new DsKalturaClient(
                conf.getString("url"),
                conf.getString("userId"),
                conf.getInteger("partnerId"),
                conf.getString("adminSecret"),
                conf.getLong("sessionKeepAliveSeconds", DEFAULT_KEEP_ALIVE));
    }
    
}
