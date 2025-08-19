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

public abstract class DsKalturaClientBase {

    // Kaltura-default: 30, maximum 500: https://developer.kaltura.com/api-docs/service/eSearch/action/searchEntry
    public static final int MAX_BATCH_SIZE = 500;
    public static final int MIN_BATCH_SIZE = 1;


    static {
        // Kaltura library uses log4j2 and will remove this error message on start up: Log4j2 could not find a logging implementation
        System.setProperty("log4j2.loggerContextFactory", "org.apache.logging.log4j.simple.SimpleLoggerContextFactory");
    }

    private Client client = null; //Client having a Kaltura session  that can be reused between API calls.
    static final Logger log = LoggerFactory.getLogger(DsKalturaClientBase.class);
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
    private int batchSize;

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
                               String adminSecret, int sessionDurationSeconds, int sessionRefreshThreshold,
                               int batchSize) throws APIException, IOException {
        this.sessionDurationSeconds = sessionDurationSeconds;
        this.sessionRefreshThreshold = sessionRefreshThreshold;
        this.sessionKeepAliveSeconds = sessionDurationSeconds - sessionRefreshThreshold;
        this.kalturaUrl = kalturaUrl;
        this.userId = userId;
        this.token = token;
        this.tokenId = tokenId;
        this.adminSecret = adminSecret;
        this.partnerId = partnerId;
        setBatchSize(batchSize);

        if (sessionKeepAliveSeconds < 600) { //Enforce some kind of reuse of session since authenticating sessions
            // will accumulate at Kaltura.
            throw new IllegalArgumentException("The difference between the configured sessionDurationSeconds and " +
                    "sessionRefreshThreshold (SessionKeepAliveSession) must be at least 600 seconds (10 minutes) ");
        }
        initializeKalturaClient();
    }

    protected int getBatchSize(){
        return batchSize;
    }

    protected void setBatchSize(int newBatchSize){
        if (newBatchSize >= MIN_BATCH_SIZE && newBatchSize <= MAX_BATCH_SIZE) {
            batchSize = newBatchSize;
        }else{
            throw new IllegalArgumentException("The batch size must be between "+ MIN_BATCH_SIZE +" and "+ MAX_BATCH_SIZE);
        }
    }

    /**
     * Builds and executes a request using the specified request builder.
     *
     * @param requestBuilder the request builder to create and execute the request
     * @param refreshSession if true, refresh the session before executing the request
     * @param <ReturnedType> the type of the response expected from the request
     * @param <SelfType>     the type of request
     * @return a Response object containing the results of the executed request
     * @throws APIException if an API error occurs during the request execution
     * @throws IOException  if an I/O error occurs during the request execution
     */
    protected <ReturnedType, SelfType extends BaseRequestBuilder<ReturnedType, SelfType>> Response<?> buildAndExecute(SelfType requestBuilder, boolean refreshSession) throws
            APIException, IOException {
        if (refreshSession) {
            getClientInstance();
        }
        RequestElement<ReturnedType> request = requestBuilder.build(client);
        return APIOkRequestsExecutor.getExecutor().execute(request);
    }

    /**
     * Handles a request using the specified request builder.
     * This method defaults to refreshing the session.
     *
     * @param requestBuilder the request builder to create and execute the request
     * @param <ReturnedType> the type of the response expected from the request
     * @param <SelfType>     the type of request
     * @return the result of the executed request
     * @throws APIException if an API error occurs during the request execution
     * @throws IOException  if an I/O error occurs during the request execution
     */
    protected <ReturnedType, SelfType extends BaseRequestBuilder<ReturnedType, SelfType>>
    ReturnedType handleRequest(SelfType requestBuilder) throws APIException, IOException {
        return handleRequest(requestBuilder, true);
    }

    /**
     * Handles a request using the specified request builder with options to refresh the session.
     *
     * @param requestBuilder the request builder to create and execute the request
     * @param refreshSession if true, refresh the session before executing the request
     * @param <ReturnedType> the type of the response expected from the request
     * @param <SelfType>     the type of request
     * @return the result of the executed request
     * @throws APIException          if an API error occurs during the request execution
     * @throws IOException           if an I/O error occurs during the request execution
     * @throws IllegalStateException if the request builder type is null
     */
    @SuppressWarnings("unchecked")
    protected <ReturnedType, SelfType extends BaseRequestBuilder<ReturnedType, SelfType>>
    ReturnedType handleRequest(SelfType requestBuilder, boolean refreshSession)
            throws APIException, IOException {
        try {
            Response<?> response = buildAndExecute(requestBuilder, refreshSession);

            if (!response.isSuccess()) {
                throw response.error;
            }
            return (ReturnedType) response.results;

        } catch (APIException e) {
            e.setMessage("Request '" + requestBuilder.getTag() + "' was unsuccessful. Reason: '" + e.getMessage() +
                    "'");
            throw e;
        }
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
        client.setKs(null); //reset session in case it ran out, else this will cause an API error.
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

        log.info("Session expiry: '{}', Session type: '{}', Privileges: '{}'", expiry,
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

    private void initializeKalturaClient() throws APIException, IOException {
        log.info("Initializing Kaltura client");
        Configuration config = new Configuration();
        config.setEndpoint(kalturaUrl);
        client = new Client(config);
        client.setPartnerId(partnerId);
        startClientSession();//Start session now to fail now rather than later if config is wrong.
    }

    /**
     * Will return a kaltura client and refresh session every sessionKeepAliveSeconds.
     * Synchronized to avoid race condition if using the DsKalturaClient class multi-threaded
     */
    private synchronized Client getClientInstance() throws IOException, APIException {
        try {
            if (System.currentTimeMillis() - lastSessionStart >= sessionKeepAliveSeconds * 1000L || client.getKs().isEmpty()) {
                log.info("Refreshing Kaltura client session, millis since last refresh:" +
                        (System.currentTimeMillis() - lastSessionStart));
                //Create the client
                startClientSession();
                lastSessionStart = System.currentTimeMillis(); //Reset timer
                log.info("Refreshed Kaltura client session");
            }
            return client;
        } catch (IOException | APIException e) {
            log.warn("Connecting to Kaltura failed. KalturaUrl={},error={}", kalturaUrl, e.getMessage());
            throw e;
        }
    }

    /**
     * Start a creates a new Kaltura session and add it to client. If secret is available in conf, it will take
     * precedent over appTokens.
     *
     * @throws Exception
     */
    private void startClientSession() throws APIException, IOException {

        String ks = null;
        if (StringUtils.isEmpty(adminSecret)) {
            log.info("Starting KalturaSession from appToken");
            ks = startAppTokenSession(SessionType.ADMIN);
        } else {
            log.warn("Starting KalturaSession from adminsecret. Use appToken instead unless you are generating " +
                    "appTokens.");
            try{
            ks = client.generateSession(adminSecret, userId, SessionType.ADMIN, partnerId,
                    sessionDurationSeconds);
            }catch (Exception e){
                throw new RuntimeException("Error starting KalturaSession from adminSecret", e);
            }
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
    private String computeHash(String token, String ks) {
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
            throw new RuntimeException("SHA-256 algorithm not available", e);
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
     * @throws IOException
     */
    private String startAppTokenSession(SessionType type) throws APIException{
        String widgetSession = startWidgetSession(sessionDurationSeconds);
        client.setKs(widgetSession);

        String hash = computeHash(token, widgetSession);
        AppTokenService.StartSessionAppTokenBuilder sessionBuilder =
                AppTokenService.startSession(tokenId, hash, null, type, sessionDurationSeconds);
        return handleRequest(sessionBuilder, false).getKs();

    }

}
