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
        String kalturaUrl=ServiceConfig.getConfig().getString("config.kaltura.url");
        String userId=ServiceConfig.getConfig().getString("config.kaltura.userId");
        int partnerId=ServiceConfig.getConfig().getInteger("config.kaltura.partnerId");        
        String adminSecret=ServiceConfig.getConfig().getString("config.kaltura.adminSecret");
        long keepAliveSeconds=ServiceConfig.getConfig().getLong("config.kaltura.sessionKeepAliveSeconds");

        DsKalturaClient client = new DsKalturaClient(kalturaUrl, userId, partnerId, adminSecret, keepAliveSeconds);               
        return client;        
    }
}
