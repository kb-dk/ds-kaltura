package dk.kb.kaltura.client;

import com.kaltura.client.APIOkRequestsExecutor;
import com.kaltura.client.Client;
import com.kaltura.client.Configuration;
import com.kaltura.client.enums.SessionType;
import com.kaltura.client.services.AppTokenService;
import com.kaltura.client.services.SessionService;
import com.kaltura.client.types.APIException;
import com.kaltura.client.types.SessionInfo;
import com.kaltura.client.types.StartWidgetSessionResponse;
import com.kaltura.client.utils.request.BaseRequestBuilder;
import com.kaltura.client.utils.request.RequestElement;
import com.kaltura.client.utils.response.base.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.Callable;

public class DsKalturaClientBase {

    // Kaltura-default: 30, maximum 500: https://developer.kaltura.com/api-docs/service/eSearch/action/searchEntry
    public static final int BATCH_SIZE = 100;
    public static final int RETRIES = 3;
    public static final int RETRY_DELAY_MILLIS = 1000;

    static {
        // Kaltura library uses log4j2 and will remove this error message on start up: Log4j2 could not find a logging implementation
        System.setProperty("log4j2.loggerContextFactory", "org.apache.logging.log4j.simple.SimpleLoggerContextFactory");
    }

    private Client client = null; //Client having a Kaltura session  that can be reused between API calls.
    static final Logger log = LoggerFactory.getLogger(DsKalturaClient.class);
    private final String kalturaUrl;
    private final String userId;
    private final int partnerId;
    private final String token;
    private final String tokenId;
    private final String adminSecret;
    private final int sessionKeepAliveSeconds;
    private long lastSessionStart = 0;
    private final int sessionRefreshThreshold;
    private final int sessionDurationSeconds;

    /**
     * Instantiate a session to Kaltura that can be used. The sessions can be reused between Kaltura calls without authenticating again.
     *
     * @param kalturaUrl              The Kaltura API url. Using the baseUrl will automatic append the API service part to the URL.
     * @param userId                  The userId that must be defined in the kaltura, userId is email xxx@kb.dk in our kaltura
     * @param partnerId               The partner id for kaltura. Kind of a collectionId.
     * @param token                   The application token used for generating client sessions
     * @param tokenId                 The id of the application token
     * @param adminSecret             The adminsecret used as password for authenticating. Must not be shared.
     * @param sessionDurationSeconds  The duration of Kaltura Session in seconds. Beware that when using AppTokens
     *                                this might have an upper bound tied to the AppToken.
     * @param sessionRefreshThreshold The threshold in seconds for session renewal.
     *                                <p>
     *                                Either a token/tokenId a adminSecret must be provided for authentication.
     * @throws IOException If session could not be created at Kaltura
     */
    public DsKalturaClientBase(String kalturaUrl, String userId, int partnerId, String token, String tokenId,
                               String adminSecret, int sessionDurationSeconds, int sessionRefreshThreshold) throws IOException {
        this.sessionDurationSeconds = sessionDurationSeconds;
        this.sessionRefreshThreshold = sessionRefreshThreshold;
        this.sessionKeepAliveSeconds = sessionDurationSeconds - sessionRefreshThreshold;
        this.kalturaUrl = kalturaUrl;
        this.userId = userId;
        this.token = token;
        this.tokenId = tokenId;
        this.adminSecret = adminSecret;
        this.partnerId = partnerId;

        if (sessionKeepAliveSeconds < 600) { //Enforce some kind of reuse of session since authenticating sessions
            // will accumulate at Kaltura.
            throw new IllegalArgumentException("The difference between the configured sessionDurationSeconds and " +
                    "sessionRefreshThreshold (SessionKeepAliveSession) must be at least 600 seconds (10 minutes) ");
        }

        getClientInstance();// Start a session already now so it will not fail later when used if credentials fails.
    }

    <T> Response<?> buildAndExecute(BaseRequestBuilder<T, ?> requestBuilder, boolean refreshSession) throws
            APIException, IOException {
        if (refreshSession) {
            getClientInstance();
        }
        RequestElement<T> request = requestBuilder.build(client);

        return retryOperation(() -> APIOkRequestsExecutor.getExecutor().execute(request), RETRIES,
                RETRY_DELAY_MILLIS, request.getTag());
    }

    <T> T handleRequest(BaseRequestBuilder<T, ?> requestBuilder) throws APIException, IOException {
        return handleRequest(requestBuilder, true);
    }

    @SuppressWarnings("unchecked")
    <T> T handleRequest(BaseRequestBuilder<T, ?> requestBuilder, boolean refreshSession)
            throws APIException, IOException {
        try {
            Response<?> response = buildAndExecute(requestBuilder, refreshSession);

            if (requestBuilder.getType() == null) {
                throw new IllegalStateException("RequestBuilder type is null");
            }
            if (!response.isSuccess()) {
                throw response.error;
            }
            return (T) response.results;

        } catch (APIException e) {
            e.setMessage(String.format("Request %s with body: %s, Reason: %s", requestBuilder.getTag(),
                    requestBuilder.getBody(), e.getMessage()));
            throw e;
        }
    }

    static <T> T retryOperation(Callable<T> operation, int retries, long delay, String operationName) {
        RuntimeException lastException = null;
        for (int attempt = 1; attempt <= retries; attempt++) {
            try {
                return operation.call(); // Try the operation
            } catch (Exception e) {
                log.error("Attempt {} of {} failed: {}", attempt, operationName, e.getClass().getSimpleName(), e);
                lastException = new RuntimeException(e); // Catch the exception and save it
                if (attempt < retries) {
                    try {
                        Thread.sleep(delay);// Wait before the next attempt
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                }
            }
        }
        assert lastException != null;
        throw lastException; // Throw the last exception if all attempts failed
    }

    /**
     * Starts widgetSession with using a client.
     *
     * @param expiry The session duration in seconds. Should not be under 600 due to caching of response on Kaltura
     *               server.
     * @return Kaltura Session
     */
    private String startWidgetSession(@Nullable Integer expiry) throws APIException, IOException {
        log.debug("Generating Widget Session...");
        client.setKs(null);
        String widgetId = "_" + client.getPartnerId();
        SessionService.StartWidgetSessionSessionBuilder requestBuilder;
        if (expiry == null) {
            requestBuilder = SessionService.startWidgetSession(widgetId);
        } else {
            requestBuilder = SessionService.startWidgetSession(widgetId, expiry);
        }
        StartWidgetSessionResponse results = handleRequest(requestBuilder, false);
        log.debug("Widget Session started successfully");

        return results.getKs();
    }

    /**
     * logs SessionInfo response from SessionService.get(ks).
     *
     * @param ks Kaltura session to log
     * @throws APIException
     * @throws IOException
     */
    public void logSessionInfo(String ks) throws APIException, IOException {

        SessionService.GetSessionBuilder requestBuilder = SessionService.get(ks);
        SessionInfo result = handleRequest(requestBuilder, false);

        // Convert Unix time to Instant
        ZonedDateTime expiry = Instant.ofEpochSecond(result.getExpiry()).atZone(ZoneId.systemDefault());

        log.info("Session expiry: {}, Session type: {}, Privileges: {}", expiry,
                result.getSessionType(), result.getPrivileges());
    }

    /**
     * logs SessionInfo response from SessionService.get(client.getKs()).
     *
     * @throws APIException
     * @throws IOException
     */
    public void logSessionInfo() throws APIException, IOException {
        logSessionInfo(client.getKs());
    }

    /**
     * Will return a kaltura client and refresh session every sessionKeepAliveSeconds.
     * Synchronized to avoid race condition if using the DsKalturaClient class multi-threaded
     */
    private synchronized Client getClientInstance() throws IOException {
        try {
            if (client == null) {
                log.info("Initializing Kaltura client");
                Configuration config = new Configuration();
                config.setEndpoint(kalturaUrl);
                client = new Client(config);
                client.setPartnerId(partnerId);
            }
            if (System.currentTimeMillis() - lastSessionStart >= sessionKeepAliveSeconds * 1000L || client.getKs().isEmpty()) {
                log.info("Refreshing Kaltura client session, millis since last refresh:" +
                        (System.currentTimeMillis() - lastSessionStart));
                //Create the client
                startClientSession();
                lastSessionStart = System.currentTimeMillis(); //Reset timer
                log.info("Refreshed Kaltura client session");
            }
            return client;
        } catch (Exception e) {
            log.warn("Connecting to Kaltura failed. KalturaUrl={},error={}", kalturaUrl, e.getMessage());
            throw new IOException(e);
        }
    }

    /**
     * Start a creates a new Kaltura session and add it to client. If secret is available in conf, it will take
     * precedent over appTokens.
     *
     * @throws Exception
     */
    private void startClientSession() throws Exception {

        String ks = null;
        if (StringUtils.isEmpty(adminSecret)) {
            log.info("Starting KalturaSession from appToken");
            ks = startAppTokenSession(SessionType.ADMIN);
        } else {
            log.warn("Starting KalturaSession from adminsecret. Use appToken instead unless you generating appTokens.");
            ks = client.generateSession(adminSecret, userId, SessionType.ADMIN, partnerId,
                    sessionDurationSeconds);
        }
        client.setKs(ks);
    }

    /**
     * Computes a SHA-256 hash of token and Kaltura Session
     *
     * @param token AppToken String for computing hash
     * @param ks    Kaltura Widget Session for computing hash
     * @return A string representing a SHA-256 tokenHash package.
     * @throws UnsupportedEncodingException
     * @throws NoSuchAlgorithmException
     */
    private String computeHash(String token, String ks) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update((ks + token).getBytes("UTF-8"));
            byte[] res = md.digest();
            StringBuilder hashString = new StringBuilder();
            for (byte i : res) {
                int decimal = (int) i & 0XFF;
                String hex = Integer.toHexString(decimal);
                if (hex.length() % 2 == 1) {
                    hex = "0" + hex;
                }
                hashString.append(hex);
            }
            return hashString.toString();
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            log.warn("SHA-256 algorithm not available");
            throw e;
        }
    }

    /**
     * Initiates a session for an application token using the provided parameters.
     * <p>
     * This method starts a widget session for the specified client, computes a hash
     * based on the provided token and the widget session, and then builds a session
     * request using the AppTokenService. The request is executed, and if successful,
     * the method returns the kaltura session (ks).
     *
     * @param type The type of session being created, represented by a
     *             {@link SessionType} enumeration.
     * @return Kaltura Session with privileges inherited from token
     * @throws APIException
     * @throws UnsupportedEncodingException
     * @throws NoSuchAlgorithmException
     */
    private String startAppTokenSession(SessionType type) throws APIException,
            IOException, NoSuchAlgorithmException {
        String widgetSession = startWidgetSession(sessionDurationSeconds);
        client.setKs(widgetSession);
        String hash = computeHash(token, widgetSession);

        AppTokenService.StartSessionAppTokenBuilder sessionBuilder =
                AppTokenService.startSession(tokenId, hash, null, type, sessionDurationSeconds);
        return handleRequest(sessionBuilder, false).getKs();
    }

}
