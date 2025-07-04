package dk.kb.kaltura.client;

import com.kaltura.client.APIOkRequestsExecutor;
import com.kaltura.client.Client;
import com.kaltura.client.Configuration;
import com.kaltura.client.enums.*;
import com.kaltura.client.services.*;
import com.kaltura.client.services.MediaService.AddContentMediaBuilder;
import com.kaltura.client.services.MediaService.AddMediaBuilder;
import com.kaltura.client.services.MediaService.DeleteMediaBuilder;
import com.kaltura.client.services.MediaService.ListMediaBuilder;
import com.kaltura.client.services.MediaService.RejectMediaBuilder;
import com.kaltura.client.services.UploadTokenService.AddUploadTokenBuilder;
import com.kaltura.client.services.UploadTokenService.UploadUploadTokenBuilder;
import com.kaltura.client.types.*;
import com.kaltura.client.utils.response.base.Response;

import dk.kb.util.webservice.exception.InternalServiceException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;


/**
 * There are two methods on DsKalturaClient:
 *  
 * <p><ul>
 * <li> API lookup and map external ID to internal Kaltura ID
 * <li> Upload a media entry (video, audio etc.) to Kaltura with meta data.
 *</ul><p>
 *
 */
public class DsKalturaClient {

    // Kaltura-default: 30, maximum 500: https://developer.kaltura.com/api-docs/service/eSearch/action/searchEntry
    public static final int BATCH_SIZE = 100;

    static {
        // Kaltura library uses log4j2 and will remove this error message on start up: Log4j2 could not find a logging implementation
        System.setProperty("log4j2.loggerContextFactory", "org.apache.logging.log4j.simple.SimpleLoggerContextFactory");
    }

    private Client client = null; //Client having a Kaltura session  that can be reused between API calls.
    private static final Logger log = LoggerFactory.getLogger(DsKalturaClient.class);
    private String kalturaUrl;
    private String userId;
    private int partnerId;
    private String token;
    private String tokenId;
    private String adminSecret;
    private int sessionKeepAliveSeconds;
    private long lastSessionStart=0;
    private int sessionRefreshThreshold;
    private int sessionDurationSeconds;

    /**
     * Instantiate a session to Kaltura that can be used. The sessions can be reused between Kaltura calls without authenticating again.
     *
     * @param kalturaUrl The Kaltura API url. Using the baseUrl will automatic append the API service part to the URL.
     * @param userId The userId that must be defined in the kaltura, userId is email xxx@kb.dk in our kaltura
     * @param partnerId The partner id for kaltura. Kind of a collectionId.
     * @param token The application token used for generating client sessions
     * @param tokenId The id of the application token
     * @param adminSecret The adminsecret used as password for authenticating. Must not be shared.
     * @param sessionDurationSeconds The duration of Kaltura Session in seconds. Beware that when using AppTokens
     *                               this might have an upper bound tied to the AppToken.
     * @param sessionRefreshThreshold The threshold in seconds for session renewal.
     *
     * Either a token/tokenId a adminSecret must be provided for authentication.
     *
     * @throws IOException  If session could not be created at Kaltura
     */
    public DsKalturaClient(String kalturaUrl, String userId, int partnerId, String token, String tokenId,
                           String adminSecret, int sessionDurationSeconds, int sessionRefreshThreshold) throws IOException {
        this.sessionDurationSeconds=sessionDurationSeconds;
        this.sessionRefreshThreshold=sessionRefreshThreshold;
        this.sessionKeepAliveSeconds=sessionDurationSeconds-sessionRefreshThreshold;
        this.kalturaUrl=kalturaUrl;
        this.userId=userId;
        this.token=token;
        this.tokenId=tokenId;
        this.adminSecret=adminSecret;
        this.partnerId=partnerId;

        if (sessionKeepAliveSeconds <600) { //Enforce some kind of reuse of session since authenticating sessions will accumulate at Kaltura.
            throw new IllegalArgumentException("SessionKeepAliveSeconds must be at least 600 seconds (10 minutes) ");
        }

        getClientInstance();// Start a session already now so it will not fail later when used if credentials fails.
    }

    /**
     * <p>
     * Delete a stream and all meta-data for the record in Kaltura.
     * It can not be restored in Kaltura and must be uploaded again if deleted by mistake.       
     * </p>
     *  
     * @param entryId The unique id in the Kaltura platform for the stream
     * @return True if record was found and deleted. False if the record with the entryId could not be found in Kaltura.
     * @throws IOException if Kaltura API called failed.
     */
    public boolean deleteStreamByEntryId(String entryId) throws IOException{                
         Client clientSession = getClientInstance();        
         DeleteMediaBuilder request = MediaService.delete(entryId);         
         Response<?> execute = APIOkRequestsExecutor.getExecutor().execute(request.build(clientSession)); // no object in response. Only status
         return execute.isSuccess();
    }
    
    
    /** 
     * <p>
     * Block a stream and all meta-data for the record in Kaltura.
     * The stream can not be played. An Kaltura administrator can still see the stream in the KMC and remove the flag it needed.  
     * In KMC refine -> moderation status -> rejected so see a list of all rejected streams and search in them      
     * </p>
     *  
     * @param entryId The unique id in the Kaltura platform for the stream
     * @return True if record was found and blocked. False if the record with the entryId could not be found in Kaltura.
     * @throws IOException if Kaltura API called failed.
     */
    public boolean blockStreamByEntryId(String entryId) throws IOException{                
         Client clientSession = getClientInstance();        
         RejectMediaBuilder request = MediaService.reject(entryId);         
         Response<?> execute = APIOkRequestsExecutor.getExecutor().execute(request.build(clientSession)); // no object in response. Only status
         return execute.isSuccess();
    }
    
    /**
     * Search Kaltura for a referenceId. The referenceId was given to Kaltura when uploading the record.<br>
     * We use filenames (file_id) as refereceIds. Example: b16bc5cb-1ea9-48d4-8e3c-2a94abae501b <br>
     * <br> 
     * The Kaltura response contains a lot more information that is required, so it is not a light weight call against Kaltura.
     *  
     * @param referenceId External reference ID given when uploading the entry to Kaltura.
     * @return The Kaltura id (internal id). Return null if the refId is not found.
     * @throws IOException if Kaltura called failed, or more than 1 entry was found with the referenceId.
     */
    @SuppressWarnings("unchecked")
    public String getKalturaInternalId(String referenceId) throws IOException{

        Client clientSession = getClientInstance();

        MediaEntryFilter filter = new MediaEntryFilter();
        filter.setReferenceIdEqual(referenceId);

        FilterPager pager = new FilterPager();
        pager.setPageIndex(1);
        pager.setPageSize(BATCH_SIZE);

        ListMediaBuilder request =  MediaService.list(filter);               

        //Getting this line correct was very hard. Little documentation and has to know which object to cast to.                
        //For some documentation about the "Kaltura search" api see: https://developer.kaltura.com/api-docs/service/media/action/list
        Response <ListResponse<MediaEntry>> response = (Response <ListResponse<MediaEntry>>) APIOkRequestsExecutor.getExecutor().execute(request.build(clientSession));
      
        //This is not normal situation. Normally Kaltura will return empty list: ({"objects":[],"totalCount":0,"objectType":"KalturaMediaListResponse"})
        // When this happens something is wrong in kaltura and we dont know if there is results or not
        if (response.results == null) {
           log.error("Unexpected NULL response from Kaltura for referenceId:"+referenceId);
            throw new InternalServiceException("Unexpected null response from Kaltura for referenceId:"+referenceId);            
        }
        
        List<MediaEntry> mediaEntries = response.results.getObjects();           

        int numberResults = mediaEntries.size();

        if ( numberResults == 0) {
            log.warn("No entry found at Kaltura for referenceId:"+referenceId);// warn since method it should not happen if given a valid referenceId 
            return null;
        }
        else if (numberResults >1) { //Sanity, has not happened yet.
            log.error("More that one entry was found at Kaltura for referenceId:"+referenceId); // if this happens there is a logic error with uploading records
            throw new IOException("More than 1 entry found at Kaltura for referenceId:"+referenceId);
        }

        return response.results.getObjects().get(0).getId();    
    }

    /**
     * Resolve Kaltura IDs for a list of referenceIDs.
     * @param referenceIds a list of {@code referenceIDs}, typically UUIDs from stream filenames.
     * @return a map from {@code referenceID} to {@code kalturaID}.
     *         Unresolvable {@code referenceIDs} will not be present in the map.
     * @throws IOException if the remote request failed.
     */
    public Map<String, String> getKalturaIds(List<String> referenceIds) throws IOException, APIException {
        if (referenceIds.isEmpty()) {
            log.info("getKulturaInternalIds(referenceIDs) called with empty list of IDs");
            return Collections.emptyMap();
        }

        List<ESearchEntryBaseItem> items = referenceIds.stream()
                .map(this::createReferenceIdItem)
                .collect(Collectors.toList());
        Response<ESearchEntryResponse> response = searchMulti(items);

        // Collect result while checking for duplicates
        final Map<String, String> pairs = new LinkedHashMap<>(referenceIds.size());
        response.results.getObjects().stream()
                .map(ESearchEntryResult::getObject)
                .forEach(entry -> {
                    String previousID;
                    if ((previousID = pairs.put(entry.getReferenceId(), entry.getId())) != null) {
                        log.warn("Warning: referenceID '{}' resolved to multiple kalturaIDs ['{}', '{}']",
                                entry.getReferenceId(), previousID, entry.getId());
                    }});
        return pairs;
    }

    /**
     * Resolve referenceIDs for a list of Kaltura IDs.
     * @param kalturaIDs a list of {@code kalturaIDs}.
     * @return a map from {@code kalturaID} to {@code referenceID}.
     *         Unresolvable {@code kalturaIDs} will not be present in the map.
     * @throws IOException if the remote request failed.
     */
    public Map<String, String> getReferenceIds(List<String> kalturaIDs) throws IOException, APIException {
        if (kalturaIDs.isEmpty()) {
            log.info("getReferenceIds(kalturaIDs) called with empty list of IDs");
            return Collections.emptyMap();
        }

        List<ESearchEntryBaseItem> items = kalturaIDs.stream()
                .map(this::createKalturaIdItem)
                .collect(Collectors.toList());
        Response<ESearchEntryResponse> response = searchMulti(items);

        return response.results.getObjects().stream()
                        .map(ESearchEntryResult::getObject)
                .collect(Collectors.toMap(BaseEntry::getId, BaseEntry::getReferenceId));
    }

    /**
     * Simple free form term search in Kaltura.
     * @param term a search term, such as {@code dr} or {@code tv avisen}.
     * @return a list of Kaltura IDs for matching records, empty if no hits. Max result size is {@link #BATCH_SIZE}.
     * @throws IOException if the remote request failed.
     */
    public List<String> searchTerm(String term) throws IOException, APIException {
        // Adapted from Java samples at https://developer.kaltura.com
        // https://developer.kaltura.com/console/service/eSearch/action/searchEntry?query=search
        // https://developer.kaltura.com/api-docs/Search--Discover-and-Personalize/esearch.html
        // TODO: This retrieves the full item representation. How to reduce to only [id, referenceId] fields?

        ESearchUnifiedItem item = new ESearchUnifiedItem();
        item.setItemType(ESearchItemType.EXACT_MATCH);
        item.searchTerm(term);
        
        return searchMulti(List.of(item)).results.getObjects().stream()
                .map(ESearchEntryResult::getObject)
                .map(BaseEntry::getId)
                .collect(Collectors.toList());
    }

    /**
     * Generic multi search for a list of {@link ESearchEntryBaseItem items},
     * returning at most {@link #BATCH_SIZE} results.
     * @param items at least 1 search item.
     * @return the response from a Kaltura search for the given items.
     * @throws IOException if the remote request failed.
     */
    @SuppressWarnings("unchecked")
    private Response<ESearchEntryResponse> searchMulti(List<ESearchEntryBaseItem> items) throws IOException, APIException {
        // Adapted from Java samples at https://developer.kaltura.com
        // https://developer.kaltura.com/console/service/eSearch/action/searchEntry?query=search
        // https://developer.kaltura.com/api-docs/Search--Discover-and-Personalize/esearch.html
        // TODO: This retrieves the full item representation. How to reduce to only [id, referenceId] fields?

        if (items.size() > BATCH_SIZE) {
            // TODO: Change this to multiple batch requests
            throw new IllegalArgumentException(
                    "Request for " + items.size() + " items exceeds current limit of " + BATCH_SIZE);
        }

        // Setup request
        ESearchEntryParams searchParams = new ESearchEntryParams();
        ESearchEntryOperator operator = new ESearchEntryOperator();
        operator.setOperator(ESearchOperatorType.OR_OP);
        searchParams.setSearchOperator(operator);
        operator.setSearchItems(items);
        FilterPager pager = new FilterPager();
        pager.setPageSize(BATCH_SIZE);

        // Issue search
        ESearchService.SearchEntryESearchBuilder requestBuilder = ESearchService.searchEntry(searchParams, pager);
        Response<ESearchEntryResponse> response = (Response<ESearchEntryResponse>)
                APIOkRequestsExecutor.getExecutor().execute(requestBuilder.build(getClientInstance()));

        if(!response.isSuccess()){
            log.debug(response.error.getMessage());
            throw response.error;
        }
        return response;
    }

    /**
     * Build a search item aka search clause for the given {@code referenceId}.
     * @param referenceID typically the UUID for a stream filename.
     * @return a search item ready for search or for building more complex search requests.
     */
    private ESearchEntryItem createReferenceIdItem(String referenceID) {
        ESearchEntryItem item = new ESearchEntryItem();
        item.setFieldName(ESearchEntryFieldName.REFERENCE_ID);
        item.searchTerm(referenceID);
        item.setItemType(ESearchItemType.EXACT_MATCH);
        return item;
    }

    /**
     * Build a search item aka search clause for the given {@code ID}.
     * @param kalturaID typically the UUID for a stream filename.
     * @return a search item ready for search or for building more complex search requests.
     */
    private ESearchEntryItem createKalturaIdItem(String kalturaID) {
        ESearchEntryItem item = new ESearchEntryItem();
        item.setFieldName(ESearchEntryFieldName.ID);
        item.searchTerm(kalturaID);
        item.setItemType(ESearchItemType.EXACT_MATCH);
        return item;
    }

    /**
     * Create empty upload token to be filled later.
     *
     * @return uploadTokenId of empty uploadToken
     * @throws IOException
     * @throws APIException
     */
    private String addUploadToken() throws IOException, APIException {
        //Get a token that will allow upload
        Client clientsession = getClientInstance();
        UploadToken uploadToken = new UploadToken();
        AddUploadTokenBuilder uploadTokenRequestBuilder = UploadTokenService.add(uploadToken);
        Response<UploadToken> response = (Response<UploadToken>)
                APIOkRequestsExecutor.getExecutor().execute(uploadTokenRequestBuilder.build(clientsession));

        if (response.isSuccess()) {
            log.debug("UploadToken '{}' successfully added.", response.results.getId());
            return response.results.getId();
        }else{
            log.debug("Adding uploadToken failed because: '{}'", response.error.getMessage());
            throw response.error;
        }
    }

    /**
     * Uploads file to Kaltura uploadToken.
     *
     * @param uploadTokenId The uploadToken created beforehand
     * @param filePath The path of file to be uploaded
     * @return The UploadTokenId when upload is complete
     * @throws IOException
     * @throws APIException
     */
    private String uploadFile(String uploadTokenId, String filePath) throws IOException, APIException {
        Client client = getClientInstance();

        //Upload the file using the upload token.
        File fileData = new File(filePath);
        boolean resume = false;
        boolean finalChunk = true;
        int resumeAt = -1;

        if (!fileData.exists() & !fileData.canRead()) {
            try {
                throw new IOException(filePath + " not accessible");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        UploadUploadTokenBuilder uploadBuilder = UploadTokenService.upload(uploadTokenId, fileData, resume, finalChunk,
                resumeAt);
        Response<UploadToken> response =
                (Response<UploadToken> ) APIOkRequestsExecutor.getExecutor().execute(uploadBuilder.build(client));
        if (response.isSuccess()) {
            log.debug("File '{}' uploaded successfully to upload token '{}'.", filePath,
                    response.results.getId());
            return response.results.getId();
        } else {
            log.debug("Failed to upload file '{}' to upload token '{}' because: '{}'", filePath,
                    uploadTokenId, response.error.getMessage());
            throw response.error;
        }

    }

    /**
     * Adds en Entry to Kaltura containing only metadata.
     *
     * @param mediaType Intended type of media. Either MediaType.AUDIO or MediaType.VIDEO
     * @param title Title of video/audio
     * @param description description
     * @param referenceId Id external from Kaltura
     * @param tag Tags on Entry. Used for ease of searching and grouping of entries within KMC.
     * @return EntryId
     * @throws IOException
     * @throws APIException
     */
    private String addEmptyEntry(MediaType mediaType, String title, String description, String referenceId, String tag) throws IOException, APIException {
        Client clientSession = getClientInstance();

        //Create entry with meta data
        MediaEntry entry = new MediaEntry();
        entry.setMediaType(mediaType);
        entry.setName(title);
        entry.setDescription(description);
        entry.setReferenceId(referenceId);
        if(tag != null) {
            entry.setTags(tag);
        }

        AddMediaBuilder addEntryBuilder = MediaService.add(entry);
        Response <MediaEntry> response = (Response <MediaEntry>)  APIOkRequestsExecutor.getExecutor().execute(addEntryBuilder.build(clientSession)); // No need for return object

        if(response.isSuccess()) {
            log.debug("Added entry '{}' successfully.", response.results.getId());
            return response.results.getId();
        }else{
            log.debug("Failed to add entry with reference ID '{}' because: '{}'", referenceId,
                    response.error.getMessage());
            throw response.error;
        }

    }


    /**
     * Adds content from an uploadToken to an Entry and return flavorID's of that flavor. If FlavorParamID is not null,
     * content is added to specified flavor within the Entry. If FlavorParamID is null content is added as source
     * flavor.
     *
     * @param uploadtokenId Upload token with content
     * @param entryId Entry to receive content
     * @param flavorParamId Id of the flavorParam where the content needs to be added within the Entry.
     * @return EntryId of updated entry
     * @throws APIException
     * @throws IOException
     */
    private String addContentToEntry(String uploadtokenId, String entryId, Integer flavorParamId) throws APIException, IOException {

        Client clientSession = getClientInstance();
        //Connect uploaded file with meta data entry
        UploadedFileTokenResource resource = new UploadedFileTokenResource();
        resource.setToken(uploadtokenId);
        AddContentMediaBuilder requestBuilder;

        if( flavorParamId == null){
            requestBuilder = MediaService.addContent(entryId, resource);
        }else{
            AssetParamsResourceContainer paramContainer = new AssetParamsResourceContainer();
            paramContainer.setAssetParamsId(flavorParamId);
            paramContainer.setResource(resource);
            requestBuilder = MediaService.addContent(entryId, paramContainer);
        }

        Response<MediaEntry> response = (Response<MediaEntry>) APIOkRequestsExecutor.getExecutor().execute(requestBuilder.build(clientSession));
        if(response.isSuccess()){
            log.debug("UploadToken '{}' was added to entry '{}'", uploadtokenId, entryId);
            return response.results.getId();
        }else{
            log.debug("UploadToken '{}' was not added to entry '{}' because: '{}'", uploadtokenId, entryId,
                    response.error.getMessage());
            throw response.error;
        }
    }

    /**
     * Upload a video or audio file to Kaltura.
     * The upload require 4 API calls to Kaltura
     * <p><ul>
     * <li> Request a upload token
     * <li> Upload file using the upload token. Get a tokenID for the upload
     * <li> Create the metadata record in Kaltura
     * <li> Connect the metadata record with the tokenID
     * </ul><p>
     * <p>
     * </ul><p>
     * <p>
     * If there for some reason happens an error after the file is uploaded and not connected to the metadata record, it does not
     * seem possible to later see the file in the kaltura administration gui. This error has only happened because I forced it.
     *
     * @param filePath    File path to the media file to upload.
     * @param referenceId Use our internal ID's there. This referenceId can be used to find the record at Kaltura and also map to internal KalturaId.
     * @param mediaType   enum type. MediaType.AUDIO or MediaType.VIDEO
     * @param title       Name/titel for the resource in Kaltura
     * @param description , optional description
     * @param tag         Optional tag. Uploads from the DS should always use tag 'DS-KALTURA'.  There is no backup for this tag in Kaltura and all uploads can be deleted easy.
     * @return The internal id for the Kaltura record. Example format: '0_jqmzfljb'
     * @throws IOException the io exception
     */
    public String uploadMedia(String filePath, String referenceId, MediaType mediaType, String title, String description,
                              String tag) throws IOException, APIException {
        return uploadMedia(filePath, referenceId, mediaType, title, description, tag, null);
    }

    /**
     * Upload a video or audio file to Kaltura.
     * The upload require 4 API calls to Kaltura
     * <p><ul>
     * <li> Request a upload token
     * <li> Upload file using the upload token. Get a tokenID for the upload
     * <li> Create the metadata record in Kaltura
     * <li> Connect the metadata record with the tokenID
     * </ul><p>
     * <p>
     * </ul><p>
     * <p>
     * If there for some reason happens an error after the file is uploaded and not connected to the metadata record, it does not
     * seem possible to later see the file in the kaltura administration gui. This error has only happened because I forced it.
     *
     * @param filePath      File path to the media file to upload.
     * @param referenceId   Use our internal ID's there. This referenceId can be used to find the record at Kaltura and also map to internal KalturaId.
     * @param mediaType     enum type. MediaType.AUDIO or MediaType.VIDEO
     * @param title         Name/titel for the resource in Kaltura
     * @param description   , optional description
     * @param tag           Optional tag. Uploads from the DS should always use tag 'DS-KALTURA'.  There is no backup for this tag in Kaltura and all uploads can be deleted easy.
     * @param flavorParamId Optional flavorParamId. This sets what flavor the file should be uploaded as. If not set flavor                 will be source, i.e. flavorParamId = 0.
     * @return The internal id for the Kaltura record. Example format: '0_jqmzfljb'
     * @throws IOException the io exception
     */
    public String uploadMedia(String filePath, String referenceId, MediaType mediaType,
    String title,String description, String tag, Integer flavorParamId) throws IOException, APIException {

        if (referenceId== null) {
            throw new IllegalArgumentException("referenceId must be defined");            
        }

        if ( mediaType == null) {
            throw new IllegalArgumentException("Kaltura mediaType must be defined");            
        }

        String uploadTokenId = addUploadToken();

        uploadFile(uploadTokenId, filePath);

        String entryId = addEmptyEntry(mediaType, title, description, referenceId, tag);

        addContentToEntry(uploadTokenId, entryId, flavorParamId);

        return entryId;
    }

    /**
     * Will return a kaltura client and refresh session every sessionKeepAliveSeconds.
     * Synchronized to avoid race condition if using the DsKalturaClient class multi-threaded
     *
     */
    private synchronized Client getClientInstance() throws IOException{
        try {
            if (this.client == null || System.currentTimeMillis()-lastSessionStart >= sessionKeepAliveSeconds*1000) {
                log.info("Refreshing Kaltura client session, millis since last refresh:"+(System.currentTimeMillis()-lastSessionStart));
                //Create the client
                Configuration config = new Configuration();
                config.setEndpoint(kalturaUrl);
                Client client = new Client(config);
                client.setPartnerId(partnerId);
                startClientSession(client, this.token, this.tokenId);
                this.client=client;
                lastSessionStart=System.currentTimeMillis(); //Reset timer
                log.info("Refreshed Kaltura client session");
                return this.client;
            }
            return this.client; //Reuse existing connection.
        }
        catch (Exception e) {
            log.warn("Connecting to Kaltura failed. KalturaUrl={},error={}",kalturaUrl,e.getMessage());
            throw new IOException (e);
        }
    }

    /**
     * Start a creates a new Kaltura session and add it to client. If secret is available in conf, it will take
     * precedent over appTokens.
     *
     * @param client The Kaltura client. Needs to be initialized with config, endpoint and partner ID
     * @param token The token of with admin privileges and SHA-256 hashType
     * @param tokenId The tokenId of token
     * @throws Exception
     */
    private void startClientSession(Client client, String token, String tokenId) throws Exception {

        String ks = null;
        if (StringUtils.isEmpty(this.adminSecret)) {
            log.info("Starting KalturaSession from appToken");
            ks = startAppTokenSession(client, tokenId, token, SessionType.ADMIN);
        } else {
            log.warn("Starting KalturaSession from adminsecret. Use appToken instead unless you generating appTokens.");
            ks = client.generateSession(adminSecret, userId, SessionType.ADMIN, this.partnerId,
                    sessionDurationSeconds);
        }

        client.setKs(ks);
    }

    /**
     * Starts widgetSession with using a client.
     * @param client The Kaltura client. Needs to be initialized with config, endpoint and partner ID
     * @param expiry The session duration in seconds. Should not be under 600 due to caching of response on Kaltura
     *               server.
     * @return Kaltura Session
     */
    public String startWidgetSession(Client client, @Nullable Integer expiry ) throws APIException {
        log.debug("Generating Widget Session...");
        client.setKs(null);
        String widgetId = "_" + client.getPartnerId();
        SessionService.StartWidgetSessionSessionBuilder requestBuilder;
        if(expiry == null) {
            requestBuilder = SessionService.startWidgetSession(widgetId);
        }else{
            requestBuilder = SessionService.startWidgetSession(widgetId, expiry);
        }
        log.debug(requestBuilder.toString());
        Response<StartWidgetSessionResponse> response =
                (Response<StartWidgetSessionResponse>) APIOkRequestsExecutor.getExecutor().execute(requestBuilder.build(client, true));

        if(!response.isSuccess()){
            throw response.error;
        }

        return response.results.getKs();
    }

    public String startWidgetSession(Client client) throws APIException, IOException {
        return startWidgetSession(client, null);
    }


    /**
     * logs SessionInfo response from SessionService.get(ks).
     *
     * @param ks Kaltura session to log
     * @throws APIException
     * @throws IOException
     */
    public void getSessionInfo(String ks) throws APIException, IOException {

        SessionService.GetSessionBuilder requestBuilder = SessionService.get(ks);

        Response<SessionInfo> response =
                (Response<SessionInfo>)APIOkRequestsExecutor.getExecutor().execute(requestBuilder.build(client));

        if(!response.isSuccess()){
            log.error(response.error.getMessage());
            return;
        }

        // Convert Unix time to LocalDateTime
        LocalDateTime localDateTime = Instant.ofEpochSecond(response.results.getExpiry())
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        // Format the LocalDateTime to a readable format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.getDefault());
        String formattedDateTime = localDateTime.format(formatter);

        log.info("Session expiry: {}, Session type: {}, Privileges: {}", formattedDateTime,
                response.results.getSessionType(), response.results.getPrivileges());
    }

    /**
     * logs SessionInfo response from SessionService.get(client.getKs()).
     *
     * @throws APIException
     * @throws IOException
     */
    public void getSessionInfo() throws APIException, IOException {
        getSessionInfo(client.getKs());
    }

    /**
     * Computes a SHA-256 hash of token and Kaltura Session
     *
     * @param token AppToken String for computing hash
     * @param ks Kaltura Widget Session for computing hash
     * @return A string representing a SHA-256 tokenHash package.
     * @throws UnsupportedEncodingException
     * @throws NoSuchAlgorithmException
     */
    private String computeHash(String token, String ks) throws UnsupportedEncodingException, NoSuchAlgorithmException {

        try{
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update((ks + token).getBytes("UTF-8"));
            byte[] res = md.digest();
            StringBuilder hashString = new StringBuilder();
            for (byte i : res) {
                int decimal = (int)i & 0XFF;
                String hex = Integer.toHexString(decimal);
                if (hex.length() % 2 == 1) {
                    hex = "0" + hex;
                }
                hashString.append(hex);
            }
            return hashString.toString();
        }catch (NoSuchAlgorithmException | UnsupportedEncodingException e){
            log.warn("SHA-256 algorithm not available");
            throw e;
        }
    }

    /**
     * Initiates a session for an application token using the provided parameters.
     *
     * This method starts a widget session for the specified client, computes a hash
     * based on the provided token and the widget session, and then builds a session
     * request using the AppTokenService. The request is executed, and if successful,
     * the method returns the kaltura session (ks).
     *
     * @param client  The client for which the session is being started. This client
     *                is used to set session-related attributes and to execute the
     *                session request.
     * @param tokenId The ID of the token for which the session is being created.
     * @param token   The token used to compute the hash for session initiation.
     * @param type    The type of session being created, represented by a
     *                {@link SessionType} enumeration.
     * @return Kaltura Session with privileges inherited from token
     * @throws APIException
     * @throws UnsupportedEncodingException
     * @throws NoSuchAlgorithmException
     */
    private String startAppTokenSession(Client client, String tokenId, String token, SessionType type) throws APIException,
            IOException, NoSuchAlgorithmException {
        String widgetSession = startWidgetSession(client, sessionDurationSeconds);
        client.setKs(widgetSession);
        String hash = computeHash(token, widgetSession);

        AppTokenService.StartSessionAppTokenBuilder sessionBuilder =
                AppTokenService.startSession(tokenId, hash,null, type, sessionDurationSeconds);
        Response<SessionInfo> response = (Response<SessionInfo>)
                APIOkRequestsExecutor.getExecutor().execute(sessionBuilder.build(client));
        if(!response.isSuccess()){
            log.debug(response.error.getMessage());
            throw response.error;
        }
        return response.results.getKs();
    }


}
