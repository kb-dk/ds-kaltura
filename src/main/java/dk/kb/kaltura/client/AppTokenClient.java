package dk.kb.kaltura.client;

import com.kaltura.client.APIOkRequestsExecutor;
import com.kaltura.client.Client;
import com.kaltura.client.Configuration;
import com.kaltura.client.enums.AppTokenHashType;
import com.kaltura.client.enums.SessionType;
import com.kaltura.client.services.AppTokenService;
import com.kaltura.client.types.AppToken;
import com.kaltura.client.types.ListResponse;
import com.kaltura.client.utils.response.base.Response;
import dk.kb.kaltura.config.ServiceConfig;

import java.util.List;

public class AppTokenClient {
    static {
        // Kaltura library uses log4j2 and will remove this error message on start up: Log4j2 could not find a logging implementation
        System.setProperty("log4j2.loggerContextFactory", "org.apache.logging.log4j.simple.SimpleLoggerContextFactory");
    }
    private Client client = null;

    public AppTokenClient(String secretKey) throws Exception {
        this.client = getKalturaClient(ServiceConfig.getKalturaUrl(),
                secretKey,
                ServiceConfig.getConfig().getString("kaltura.userId"),
                ServiceConfig.getConfig().getInteger("kaltura.partnerId"));
    }


    public AppToken addAppToken(String description) {
        AppToken appToken = new AppToken();
        appToken.setDescription(description);
        appToken.setHashType(AppTokenHashType.SHA256);
        appToken.setSessionType(SessionType.ADMIN);

        AppTokenService.AddAppTokenBuilder requestBuilder = AppTokenService.add(appToken);
        Response<AppToken> response = (Response<AppToken>) APIOkRequestsExecutor.getExecutor().execute(requestBuilder.build(client));
        if (response.isSuccess()) {
            return response.results;
        }
        throw new RuntimeException("Add app token failed",response.error);
    }


    public List<AppToken> listAppTokens() {
        AppTokenService.ListAppTokenBuilder requestBuilder = AppTokenService.list();
        Response<?> response = APIOkRequestsExecutor.getExecutor().execute(requestBuilder.build(client));
        if (response.isSuccess()) {
            return ((ListResponse) response.results).getObjects();
        }
        throw new RuntimeException("List app tokens failed ",response.error);
    }

    public void deleteAppToken(String appTokenId) {
        AppTokenService.DeleteAppTokenBuilder requestBuilder = AppTokenService.delete(appTokenId);
        Response<?> response = APIOkRequestsExecutor.getExecutor().execute(requestBuilder.build(client));
        if (!response.isSuccess()) {
            throw new RuntimeException("Delete app token failed",response.error);
        }
    }


    private Client getKalturaClient(String url, String secretKey, String userId, int partnerId) throws Exception {
        Configuration config = new Configuration();
        config.setEndpoint(url);
        Client client = new Client(config);
        String ks = client.generateSession(secretKey,userId, SessionType.ADMIN,partnerId);
        client.setKs(ks);
        return client;
    }


}
