package dk.kb.kaltura.jobs;

import java.io.IOException;

import dk.kb.kaltura.client.DsKalturaClient;
import dk.kb.kaltura.config.ServiceConfig;

/**
 * Abstract class with common features required for the different jobs. All jobs extends this class
 *  
 */
public abstract class JobsBase {

    public static DsKalturaClient getKalturaClient() throws IOException {

        ServiceConfig.initialize(System.getProperty("dk.kb.applicationConfig"));        
        String kalturaUrl=ServiceConfig.getConfig().getString("kaltura.url");
        String userId=ServiceConfig.getConfig().getString("kaltura.userId");
        int partnerId=ServiceConfig.getConfig().getInteger("kaltura.partnerId");        
        String appToken=ServiceConfig.getConfig().getString("kaltura.appToken");
        String appTokenId=ServiceConfig.getConfig().getString("kaltura.appTokenId");
        long keepAliveSeconds=ServiceConfig.getConfig().getLong("kaltura.sessionKeepAliveSeconds");

        DsKalturaClient client = new DsKalturaClient(kalturaUrl, userId, partnerId, appToken, appTokenId, keepAliveSeconds);
        return client;        
    }
}
