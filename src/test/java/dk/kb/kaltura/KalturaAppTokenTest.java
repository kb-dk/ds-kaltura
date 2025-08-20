package dk.kb.kaltura;

import com.kaltura.client.types.AppToken;
import dk.kb.kaltura.client.AppTokenClient;
import dk.kb.kaltura.config.ServiceConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

@Tag("integration")
public class KalturaAppTokenTest {
    private static final Logger log = LoggerFactory.getLogger(KalturaAppTokenTest.class);

    private static final int DEFAULT_SESSION_DURATION_SECONDS = 86400;
    private static final int DEFAULT_REFRESH_THRESHOLD = 3600;

    @BeforeAll
    public static void setup() throws IOException {
        ServiceConfig.initialize("src/main/conf/ds-kaltura-*.yaml"); // Does not seem like a solid construction
    }


    @Test
    public void listAppTokens() throws Exception {
        AppTokenClient client = new AppTokenClient(ServiceConfig.getConfig().getString("kaltura.adminSecret"));
        List<AppToken> tokens = client.listAppTokens();
        tokens.stream().forEach((appToken) -> {
            System.out.println("token:" + appToken.getToken() + " tokenId: " + appToken.getId() + " " + appToken.getCreatedAt() + " " + appToken.getExpiry() + " " + appToken.getSessionUserId() + " " + appToken.getDescription());
        });
    }

    @Test
    public void addAppToken() throws Exception {
        AppTokenClient client = new AppTokenClient(ServiceConfig.getConfig().getString("kaltura.adminSecret"));
        AppToken appToken = client.addAppToken("description");
        System.out.println(appToken.getId() + " " + appToken.getToken() + " " + appToken.getExpiry() + " " + appToken.getDescription());
    }

    @Test
    public void deleteAppToken() throws Exception {
        AppTokenClient client = new AppTokenClient(ServiceConfig.getConfig().getString("kaltura.adminSecret"));
        client.deleteAppToken("");
    }

}
