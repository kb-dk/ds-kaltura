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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    private long sessionKeepAliveSeconds;
    private long lastSessionStart=0;

    /**
     * Instantiate a session to Kaltura that can be used. The sessions can be reused between Kaltura calls without authenticating again.
     *
     * @param kalturaUrl The Kaltura API url. Using the baseUrl will automatic append the API service part to the URL.
     * @param userId The userId that must be defined in the kaltura, userId is email xxx@kb.dk in our kaltura
     * @param partnerId The partner id for kaltura. Kind of a collectionId.
     * @param token The application token used for generating client sessions
     * @param tokenId The id of the application token
     * @param adminSecret The adminsecret used as password for authenticating. Must not be shared.
     * @param sessionKeepAliveSeconds Reuse the Kaltura Session for performance. Sessions will be refreshed at the given interval. Recommended value 86400 (1 day)
     *
     * Either a token/tokenId a adminSecret must be provided for authentication.
     *
     * @throws IOException  If session could not be created at Kaltura
     */
    public DsKalturaClient(String kalturaUrl, String userId, int partnerId, String token, String tokenId, String adminSecret, long sessionKeepAliveSeconds) throws IOException {
        if (sessionKeepAliveSeconds <600) { //Enforce some kind of reuse of session since authenticating sessions will accumulate at Kaltura.
            throw new IllegalArgumentException("SessionKeepAliveSeconds must be at least 600 seconds (10 minutes) ");
        }               
        this.kalturaUrl=kalturaUrl;
        this.userId=userId;
        this.token=token;
        this.tokenId=tokenId;
        this.adminSecret = adminSecret;
        this.partnerId=partnerId;
        this.sessionKeepAliveSeconds=sessionKeepAliveSeconds;
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
    public String getKulturaInternalId(String referenceId) throws IOException{

        Client clientSession = getClientInstance();

        MediaEntryFilter filter = new MediaEntryFilter();
        filter.setReferenceIdEqual(referenceId);

        FilterPager pager = new FilterPager();
        pager.setPageIndex(10);

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
    public Map<String, String> getKulturaIds(List<String> referenceIds) throws IOException{
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
                        log.warn("Warning: referenceID '{}' resolved to multiple kalturaIDs [\"{}\", \"{}\"]",
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
    public Map<String, String> getReferenceIds(List<String> kalturaIDs) throws IOException{
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
    public List<String> searchTerm(String term) throws IOException{
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
    private Response<ESearchEntryResponse> searchMulti(List<ESearchEntryBaseItem> items) throws IOException{
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
        return (Response<ESearchEntryResponse>)
                APIOkRequestsExecutor.getExecutor().execute(requestBuilder.build(getClientInstance()));
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
                              String tag) throws IOException{
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
    @SuppressWarnings("unchecked")
    public String uploadMedia(String filePath, String referenceId, MediaType mediaType, String title, String description,
                              String tag, Integer flavorParamId) throws IOException{

        if (referenceId== null) {
            throw new IllegalArgumentException("referenceId must be defined");            
        }

        if ( mediaType == null) {
            throw new IllegalArgumentException("Kaltura mediaType must be defined");            
        }

        Client clientSession = getClientInstance();

        //Get a token that will allow upload        
        UploadToken uploadToken = new UploadToken();
        AddUploadTokenBuilder uploadTokenRequestBuilder = UploadTokenService.add(uploadToken);              
        Response <UploadToken> response = (Response <UploadToken>) APIOkRequestsExecutor.getExecutor().execute(uploadTokenRequestBuilder.build(clientSession));        
        String tokenId=response.results.getId();

        //Upload the file using the upload token.        
        File fileData = new File(filePath);
        boolean resume = false;
        boolean finalChunk = true;
        int resumeAt = -1;

        UploadUploadTokenBuilder uploadBuilder = UploadTokenService.upload(tokenId, fileData, resume, finalChunk, resumeAt);
        APIOkRequestsExecutor.getExecutor().execute(uploadBuilder.build(clientSession)); //No need for return object

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
        Response <MediaEntry> response1 = (Response <MediaEntry>)  APIOkRequestsExecutor.getExecutor().execute(addEntryBuilder.build(clientSession)); // No need for return object
        String entryId= response1.results.getId();

        //Connect uploaded file with meta data entry       
        UploadedFileTokenResource resource = new UploadedFileTokenResource();
        resource.setToken(tokenId);

        AddContentMediaBuilder requestBuilder;
        if( flavorParamId == null){
            requestBuilder = MediaService.addContent(entryId, resource);
        }else{
            AssetParamsResourceContainer paramContainer = new AssetParamsResourceContainer();
            paramContainer.setAssetParamsId(flavorParamId);
            paramContainer.setResource(resource);
            requestBuilder = MediaService.addContent(entryId, paramContainer);
        }
        APIOkRequestsExecutor.getExecutor().queue(requestBuilder.build(clientSession));

        return entryId;
    }

    /**
     * Will return a kaltura client and refresh session every sessionKeepAliveSeconds.
     * Synchronized to avoid race condition if using the DsKalturaClient class multi-threaded
     *
     */
    private synchronized Client getClientInstance() throws IOException{
        try {


            if (client == null || System.currentTimeMillis()-lastSessionStart >= sessionKeepAliveSeconds*1000) {
                log.info("Refreshing Kaltura client session, millis since last refresh:"+(System.currentTimeMillis()-lastSessionStart));
                //Create the client
                //KalturaConfiguration config = new KalturaConfiguration();
                Configuration config = new Configuration();
                config.setEndpoint(kalturaUrl);
                Client client = new Client(config);
                client.setPartnerId(partnerId);
                startClientSession(client, this.token, this.tokenId);
                this.client=client;
                lastSessionStart=System.currentTimeMillis(); //Reset timer
                log.info("Refreshed Kaltura client session");
                return client;
            }
            return client; //Reuse existing connection.
        }
        catch (Exception e) {
            log.warn("Connecting to Kaltura failed. KalturaUrl={},error={}",kalturaUrl,e.getMessage());
            throw new IOException (e);
        }
    }

    /*
     * Sets client session to a privileged session using appToken.
     */
    private void startClientSession(Client client, String token, String tokenId) throws Exception {
        String widgetSession = generateWidgetSession(client);
        client.setKs(widgetSession);
        String hash = computeHash(client, token, widgetSession);
        String ks = null;
        if (StringUtils.isEmpty(this.adminSecret)) {
            log.info("Starting KalturaSession from appToken");
            ks = startAppTokenSession(hash, client, tokenId);
        } else {
            log.warn("Starting KalturaSession from adminsecret. Use appToken instead unless you generating appTokens.");
            ks = client.generateSession(adminSecret, userId, SessionType.ADMIN, partnerId);
        }

        client.setKs(ks);
    }

    private String generateWidgetSession(Client client) {
        log.debug("Generating Widget Session...");
        String widgetId = "_" + client.getPartnerId();
        int expiry = Client.EXPIRY;
        SessionService.StartWidgetSessionSessionBuilder requestBuilder =
                SessionService.startWidgetSession(widgetId,
                        expiry);
        var request = requestBuilder.build(client);
        Response<StartWidgetSessionResponse> response =
                (Response<StartWidgetSessionResponse>) APIOkRequestsExecutor.getExecutor().execute(request);
        return response.results.getKs();
    }


    /**
     * @param token AppToken String for computing hash
     * @param ks    Unprivileged Kaltura Widget Session for computing hash
     * @return A string representing a tokenHash package or an empty string if Error Occurs.
     */
    private String computeHash(Client client, String token, String ks){
        client.setSessionId(ks);
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
        }
        return "";
    }

    private String startAppTokenSession(String hash, Client client, String tokenId) {
        AppTokenService.StartSessionAppTokenBuilder sessionBuilder =
                AppTokenService.startSession(tokenId, hash);
        sessionBuilder.type(SessionType.ADMIN.name());
        Response<SessionInfo> response = (Response<SessionInfo>)
                APIOkRequestsExecutor.getExecutor().execute(sessionBuilder.build(client));
        return response.results.getKs();
    }


}
