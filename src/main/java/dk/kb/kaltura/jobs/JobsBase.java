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
        String token=ServiceConfig.getConfig().getString("kaltura.token");
        String tokenId=ServiceConfig.getConfig().getString("kaltura.tokenId");
        String adminSecret=ServiceConfig.getConfig().getString("kaltura.adminSecret");
        int sessionDurationSeconds = ServiceConfig.getConfig().getInteger("sessionDurationSeconds");
        int sessionRefreshThreshold = ServiceConfig.getConfig().getInteger("sessionRefreshThreshold");

        DsKalturaClient client = new DsKalturaClient(kalturaUrl, userId, partnerId, token, tokenId, adminSecret,
                sessionDurationSeconds, sessionRefreshThreshold);
        return client;        
    }
}
