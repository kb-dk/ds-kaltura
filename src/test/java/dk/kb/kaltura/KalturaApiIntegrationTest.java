package dk.kb.kaltura;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaltura.client.enums.MediaType;

import dk.kb.kaltura.client.DsKalturaClient;


/**
 * Unittest that will call the API search method. Search for a local refenceId to get the Kaltura internal id for the record.
 * Using Kaltura client v.19.3.3 there is no longer sporadic errors when calling the API.
 * 
 */
@Tag("integration")
public class KalturaApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(KalturaApiIntegrationTest.class);
    
    private String kalturaUrl="https://kmc.kaltura.nordu.net";
    private int partnerId=399; // The partnerId we have to use. Not secret
    private String userId="";// <-- Change to valid value
    private long sessionKeepAliveSeconds=86400;
    private String token = ""; // <-- Change to valid value. DO NOT CHECK THE SECRET INTO GITHUB.
    private String tokenId = ""; //  <-- Change to valid value. DO NOT CHECK THE SECRET INTO GITHUB.

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
        return new DsKalturaClient(kalturaUrl,userId,partnerId,token,tokenId,sessionKeepAliveSeconds);
    }
    
}
