package dk.kb.kaltura.client;

import com.kaltura.client.enums.ESearchEntryFieldName;
import com.kaltura.client.enums.ESearchItemType;
import com.kaltura.client.enums.ESearchOperatorType;
import com.kaltura.client.enums.MediaType;
import com.kaltura.client.services.BaseEntryService;
import com.kaltura.client.services.ESearchService;
import com.kaltura.client.services.MediaService;
import com.kaltura.client.services.MediaService.AddContentMediaBuilder;
import com.kaltura.client.services.MediaService.DeleteMediaBuilder;
import com.kaltura.client.services.MediaService.RejectMediaBuilder;
import com.kaltura.client.services.UploadTokenService;
import com.kaltura.client.types.*;
import com.kaltura.client.utils.request.BaseRequestBuilder;
import com.kaltura.client.utils.response.base.Response;

import java.io.File;
import java.io.IOException;
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
 * </ul><p>
 */
public class DsKalturaClient extends DsKalturaClientBase {

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
    public DsKalturaClient(String kalturaUrl, String userId, int partnerId, String token, String tokenId,
                           String adminSecret, int sessionDurationSeconds, int sessionRefreshThreshold) throws APIException {
        super(kalturaUrl, userId, partnerId, token, tokenId, adminSecret, sessionDurationSeconds,
                sessionRefreshThreshold, MAX_BATCH_SIZE);
    }

    /**
     * Retrieves a {@link BaseEntry} object based on the provided entry ID.
     * <p>
     * This method handles the API request to fetch the entry from the service.
     * It calls the static method {@link BaseEntryService#get(String)} to initiate
     * the retrieval process and then processes the response using the
     * {@link DsKalturaClientBase#handleRequest(BaseRequestBuilder)} method.
     *
     * @param entryId the ID of the entry to be retrieved
     * @return a {@link BaseEntry} object representing the requested entry
     * @throws APIException if there is an error related to the API request,
     *                      such as an invalid entry ID or server-side issues
     */
    public BaseEntry getEntry(String entryId) throws APIException {
        return handleRequest(BaseEntryService.get(entryId));
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
    public boolean deleteStreamByEntryId(String entryId) throws APIException {
        DeleteMediaBuilder request = MediaService.delete(entryId);
        return buildAndExecute(request, true).isSuccess(); // no object in response. Only status
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
    public boolean blockStreamByEntryId(String entryId) throws APIException {
        RejectMediaBuilder request = MediaService.reject(entryId);
        return buildAndExecute(request, true).isSuccess();
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
    public String getKalturaInternalId(String referenceId) throws IOException, APIException {
        List<ESearchEntryBaseItem> items = List.of(createReferenceIdItem(referenceId));
        ESearchEntryResponse response = handleRequest(getSearchEntryESearchBuilder(items));
        int numberResults = response.getTotalCount();

        if (numberResults == 0) {
            log.info("No entry found at Kaltura for referenceId:'{}'", referenceId);
            return null;
        } else if (numberResults > 1) { //Sanity, has not happened yet.
            log.error("More that one entry was found at Kaltura for referenceId:'{}'", referenceId); // if this happens there is a logic error with uploading records
            throw new IOException("More than 1 entry found at Kaltura for referenceId:" + referenceId);
        }

        return response.getObjects().get(0).getObject().getId();
    }

    /**
     * Resolve Kaltura IDs for a list of referenceIDs.
     *
     * @param referenceIds a list of {@code referenceIDs}, typically UUIDs from stream filenames.
     * @return a map from {@code referenceID} to {@code kalturaID}.
     * Unresolvable {@code referenceIDs} will not be present in the map.
     * @throws IOException if the remote request failed.
     */
    public Map<String, String> getKalturaIds(List<String> referenceIds) throws APIException {
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
                    }
                });
        return pairs;
    }

    /**
     * Resolve referenceIDs for a list of Kaltura IDs.
     *
     * @param kalturaIDs a list of {@code kalturaIDs}.
     * @return a map from {@code kalturaID} to {@code referenceID}.
     * Unresolvable {@code kalturaIDs} will not be present in the map.
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
     *
     * @param term a search term, such as {@code dr} or {@code tv avisen}.
     * @return a list of Kaltura IDs for matching records, empty if no hits. Max result size is {@link #getBatchSize()}.
     * @throws IOException if the remote request failed.
     */
    public List<String> searchTerm(String term) throws IOException, APIException {
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
     * returning at most {@link #getBatchSize()} results.
     *
     * @param items at least 1 search item.
     * @return the response from a Kaltura search for the given items.
     * @throws IOException if the remote request failed.
     */
    @SuppressWarnings("unchecked")
    private Response<ESearchEntryResponse> searchMulti(List<ESearchEntryBaseItem> items) throws APIException {
        Response<?> response = buildAndExecute(getSearchEntryESearchBuilder(items), true);
        return (Response<ESearchEntryResponse>) response;
    }

    /**
     * Creates a search entry builder for the given list of ESearchEntryBaseItems.
     *
     * <p>Validates that the list size does not exceed the current batch size limit.
     * Configures search parameters with an OR operator and sets up pagination.</p>
     *
     * @param items a list of {@link ESearchEntryBaseItem} to search. Must not exceed the batch size limit.
     * @return an instance of {@link ESearchService.SearchEntryESearchBuilder} configured for the search.
     * @throws IllegalArgumentException if the size of {@code items} exceeds the batch size limit.
     */
    private ESearchService.SearchEntryESearchBuilder getSearchEntryESearchBuilder(List<ESearchEntryBaseItem> items) {
        if (items.size() > getBatchSize()) {
            throw new IllegalArgumentException(
                    "Request for " + items.size() + " items exceeds current limit of " + getBatchSize());
        }

        // Setup request
        ESearchEntryParams searchParams = new ESearchEntryParams();
        ESearchEntryOperator operator = new ESearchEntryOperator();
        operator.setOperator(ESearchOperatorType.OR_OP);
        searchParams.setSearchOperator(operator);
        operator.setSearchItems(items);
        FilterPager pager = new FilterPager();
        pager.setPageSize(getBatchSize());

        return ESearchService.searchEntry(searchParams, pager);
    }

    /**
     * Build a search item aka search clause for the given {@code referenceId}.
     *
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
     *
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
    private String addUploadToken() throws APIException {
        //Get a token that will allow upload
        UploadToken uploadToken = new UploadToken();
        try {
            UploadToken results = handleRequest(UploadTokenService.add(uploadToken));
            log.debug("UploadToken '{}' successfully added.", results.getId());
            return results.getId();
        } catch (Exception e) {
            log.warn("Adding uploadToken failed because: '{}'", e.getMessage());
            throw e;
        }
    }

    /**
     * Uploads file to Kaltura uploadToken.
     *
     * @param uploadTokenId The uploadToken created beforehand
     * @param filePath      The path of file to be uploaded
     * @return The UploadTokenId when upload is complete
     * @throws IOException
     * @throws APIException
     */
    private String uploadFile(String uploadTokenId, String filePath) throws APIException, IOException {
        //Upload the file using the upload token.
        File fileData = new File(filePath);
        boolean resume = false;
        boolean finalChunk = true;
        int resumeAt = -1;

        if (!fileData.exists() & !fileData.canRead()) {
            throw new IOException(filePath + " not accessible");
        }

        try {
            UploadToken results = handleRequest(UploadTokenService.upload(uploadTokenId, fileData, resume, finalChunk,
                    resumeAt));
            log.debug("File '{}' uploaded successfully to upload token '{}'.", filePath,
                    results.getId());
            return results.getId();
        } catch (APIException e) {
            log.warn("Failed to upload file '{}' to upload token '{}' because: '{}'", filePath,
                    uploadTokenId, e.getMessage());
            throw e;
        }
    }

    /**
     * Adds en Entry to Kaltura containing only metadata.
     *
     * @param mediaType   Intended type of media. Either MediaType.AUDIO or MediaType.VIDEO
     * @param title       Title of video/audio
     * @param description description
     * @param referenceId Id external from Kaltura
     * @param tag         Tags on Entry. Used for ease of searching and grouping of entries within KMC.
     * @return EntryId
     * @throws IOException
     * @throws APIException
     */
    private String addEmptyEntry(MediaType mediaType, String title, String description, String referenceId,
                                 String tag) throws APIException {
        //Create entry with meta data
        MediaEntry entry = new MediaEntry();
        entry.setMediaType(mediaType);
        entry.setName(title);
        entry.setDescription(description);
        entry.setReferenceId(referenceId);
        if (tag != null) {
            entry.setTags(tag);
        }

        try {
            MediaEntry results = handleRequest(MediaService.add(entry));
            log.debug("Added entry '{}' successfully.", results.getId());
            return results.getId();
        } catch (APIException e) {
            log.warn("Failed to add entry with reference ID '{}' because: '{}'", referenceId,
                    e.getMessage());
            throw e;
        }
    }


    /**
     * Adds content from an uploadToken to an Entry and return flavorID's of that flavor. If FlavorParamID is not null,
     * content is added to specified flavor within the Entry. If FlavorParamID is null content is added as source
     * flavor.
     *
     * @param uploadtokenId Upload token with content
     * @param entryId       Entry to receive content
     * @param flavorParamId Id of the flavorParam where the content needs to be added within the Entry.
     * @return EntryId of updated entry
     * @throws APIException
     * @throws IOException
     */
    private String addContentToEntry(String uploadtokenId, String entryId, Integer flavorParamId)
            throws APIException {

        //Connect uploaded file with meta data entry
        UploadedFileTokenResource resource = new UploadedFileTokenResource();
        resource.setToken(uploadtokenId);
        AddContentMediaBuilder requestBuilder;

        if (flavorParamId == null) {
            requestBuilder = MediaService.addContent(entryId, resource);
        } else {
            AssetParamsResourceContainer paramContainer = new AssetParamsResourceContainer();
            paramContainer.setAssetParamsId(flavorParamId);
            paramContainer.setResource(resource);
            requestBuilder = MediaService.addContent(entryId, paramContainer);
        }

        try {
            return handleRequest(requestBuilder).getId();
        } catch (APIException e) {
            log.warn("UploadToken '{}' was not added to entry '{}' because: '{}'", uploadtokenId, entryId,
                    e.getMessage());
            throw e;
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
                              String title, String description, String tag, Integer flavorParamId)
            throws IOException, APIException {

        if (referenceId == null) {
            throw new IllegalArgumentException("referenceId must be defined");
        }
        if (mediaType == null) {
            throw new IllegalArgumentException("Kaltura mediaType must be defined");
        }

        String uploadTokenId = addUploadToken();
        uploadFile(uploadTokenId, filePath);
        String entryId = addEmptyEntry(mediaType, title, description, referenceId, tag);
        addContentToEntry(uploadTokenId, entryId, flavorParamId);

        return entryId;
    }

}
