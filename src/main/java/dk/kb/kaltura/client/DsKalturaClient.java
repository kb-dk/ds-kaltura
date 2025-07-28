package dk.kb.kaltura.client;

import com.kaltura.client.enums.*;
import com.kaltura.client.services.*;
import com.kaltura.client.services.MediaService.*;
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
public class DsKalturaClient extends DsKalturaClientBase{

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
    public DsKalturaClient(String kalturaUrl, String userId, int partnerId, String token, String tokenId, String adminSecret, int sessionDurationSeconds, int sessionRefreshThreshold) throws IOException {
        super(kalturaUrl, userId, partnerId, token, tokenId, adminSecret, sessionDurationSeconds, sessionRefreshThreshold);
    }

    public BaseEntry getEntry(String entryId) throws APIException, IOException {
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
    public boolean deleteStreamByEntryId(String entryId) throws IOException, APIException {
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
    public boolean blockStreamByEntryId(String entryId) throws IOException {
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
    @SuppressWarnings("unchecked")
    public String getKalturaInternalId(String referenceId) throws IOException, APIException {

        MediaEntryFilter filter = new MediaEntryFilter();
        filter.setReferenceIdEqual(referenceId);

        FilterPager pager = new FilterPager();
        pager.setPageIndex(1);
        pager.setPageSize(BATCH_SIZE);

        ListMediaBuilder request = MediaService.list(filter);
        ListResponse<MediaEntry> results = handleRequest(request);

        List<MediaEntry> mediaEntries = results.getObjects();

        int numberResults = mediaEntries.size();

        if (numberResults == 0) {
            log.warn("No entry found at Kaltura for referenceId:" + referenceId);// warn since method it should not happen if given a valid referenceId
            return null;
        } else if (numberResults > 1) { //Sanity, has not happened yet.
            log.error("More that one entry was found at Kaltura for referenceId:" + referenceId); // if this happens there is a logic error with uploading records
            throw new IOException("More than 1 entry found at Kaltura for referenceId:" + referenceId);
        }

        return results.getObjects().get(0).getId();
    }

    /**
     * Resolve Kaltura IDs for a list of referenceIDs.
     *
     * @param referenceIds a list of {@code referenceIDs}, typically UUIDs from stream filenames.
     * @return a map from {@code referenceID} to {@code kalturaID}.
     * Unresolvable {@code referenceIDs} will not be present in the map.
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
     * @param term a search term, such as {@code dr} or {@code tv avisen}.
     * @return a list of Kaltura IDs for matching records, empty if no hits. Max result size is {@link #BATCH_SIZE}.
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
     * returning at most {@link #BATCH_SIZE} results.
     * @param items at least 1 search item.
     * @return the response from a Kaltura search for the given items.
     * @throws IOException if the remote request failed.
     */
    @SuppressWarnings("unchecked")
    private Response<ESearchEntryResponse> searchMulti(List<ESearchEntryBaseItem> items) throws IOException, APIException {
        if (items.size() > BATCH_SIZE) {
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
        return (Response<ESearchEntryResponse>) buildAndExecute(requestBuilder, true);
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
    private String addUploadToken() throws IOException, APIException {
        //Get a token that will allow upload
        UploadToken uploadToken = new UploadToken();
        try{
            UploadToken results = handleRequest(UploadTokenService.add(uploadToken));
            log.debug("UploadToken '{}' successfully added.", results.getId());
            return results.getId();
        } catch (Exception e){
            log.debug("Adding uploadToken failed because: '{}'", e.getMessage());
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
    private String uploadFile(String uploadTokenId, String filePath) throws IOException, APIException {
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

        try {
            UploadToken results = handleRequest(UploadTokenService.upload(uploadTokenId, fileData, resume, finalChunk,
                resumeAt));
            log.debug("File '{}' uploaded successfully to upload token '{}'.", filePath,
                    results.getId());
            return results.getId();
        } catch (APIException | IOException e) {
            log.debug("Failed to upload file '{}' to upload token '{}' because: '{}'", filePath,
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
    private String addEmptyEntry(MediaType mediaType, String title, String description, String referenceId, String tag) throws IOException, APIException {
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
        } catch (APIException | IOException e){
            log.debug("Failed to add entry with reference ID '{}' because: '{}'", referenceId,
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
            throws APIException, IOException {

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

        return handleRequest(requestBuilder).getId();
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

    public Map<String,List<List<String>>> getReportTableFromSegments(List<String> segments,
                                                           ReportInputFilter reportInputFilter) throws APIException, IOException {

//        List<List<String>> rows = new ArrayList<>();

        if (reportInputFilter.getEntryCreatedAtGreaterThanOrEqual() != null){
            log.warn("Filter option EntryCreatedAtGreaterThanOrEqual will be overwritten with segments");
        }
        if (reportInputFilter.getEntryCreatedAtLessThanOrEqual() != null){
            log.warn("Filter option EntryCreatedAtLessThanOrEqual will be overwritten with segments");
        }


        Map<String, List<List<String>>> segmentMap = new HashMap<>();
        for(int i = 0; i < segments.size()-1; i++){
            long start = Long.parseLong(segments.get(i));
            long end ;
            reportInputFilter.setEntryCreatedAtGreaterThanOrEqual(start);
            if (i+1 > segments.size()-1) {
                end = Long.MAX_VALUE;
                reportInputFilter.setEntryCreatedAtLessThanOrEqual(end);
            }else {
                end = Long.parseLong(segments.get(i + 1))-1;
                reportInputFilter.setEntryCreatedAtLessThanOrEqual(end);
            }

            List<List<String>> tmp = getReportTable(ReportType.TOP_CONTENT, reportInputFilter,
                    ReportOrderBy.CREATED_AT_ASC.getValue());

//            if (!tmp.isEmpty()) {
//                if (rows.isEmpty()) { //add all content if first non-empty response
//                    rows.addAll(tmp);
//                }else{ //add all content but header
//                    rows.addAll(tmp.subList(1, tmp.size()));
//                }
//            }
            segmentMap.put(start+"-"+end, tmp);
            log.debug("Done with segment {} of {}: {} - {}", i+1, segments.size(),reportInputFilter.getEntryCreatedAtGreaterThanOrEqual(),
                    reportInputFilter.getEntryCreatedAtLessThanOrEqual());
        }

        return segmentMap;
    }
    public String getUrlForReportAsCsv(String title, String reportText, String headers,
                                       ReportType reportType,
                                       ReportInputFilter reportInputFilter, String dimension, FilterPager pager,
                                             String order, String objectIds, ReportResponseOptions responseOptions)
            throws IOException, APIException {

        Client client = getClientInstance();

        ReportService.GetUrlForReportAsCsvReportBuilder requestBuilder = ReportService.getUrlForReportAsCsv(title, reportText, headers, reportType,
                reportInputFilter, dimension, pager, order, objectIds,  responseOptions);

        Response<?> response = APIOkRequestsExecutor.getExecutor().execute(requestBuilder.build(client));

        if(!response.isSuccess()){
            throw response.error;
        }
        if(!(response.results instanceof String)){
            throw new RuntimeException("Not a String");
        }

        return (String) response.results;
    }


    public void exportTopContent(ReportInputFilter filter, String email) throws APIException, IOException {

        ReportExportItem reportExportItem = createExportItem(ReportExportItemType.TABLE, ReportType.TOP_CONTENT,
               filter);
        List<ReportExportItem> exportItems = new ArrayList<>();
        exportItems.add(reportExportItem);

        exportReportCSV(null, email, exportItems);
        log.debug("Done with export TOP_CONTENT request!");
    }

    private ReportExportItem createExportItem(ReportExportItemType action,
                                              ReportType type, ReportInputFilter filter){
        ReportExportItem exportItem = new ReportExportItem();
        exportItem.setAction(action);
        exportItem.setFilter(filter);
        exportItem.setReportType(type);
        exportItem.setReportTitle(exportItem.getReportType().name());
        return exportItem;
    }

    public ReportExportResponse exportReportCSV(String baseUrl, String email, List<ReportExportItem> reportExportItems) throws APIException, IOException {
        Client client = getClientInstance();

        ReportExportParams exportParams = new ReportExportParams();
        exportParams.setRecipientEmail(email);
        exportParams.setBaseUrl(baseUrl);
        exportParams.setReportItems(reportExportItems);
        exportParams.setTimeZoneOffset(-120);

        var requestBuilder = ReportService.exportToCsv(exportParams);

        Response<?> response = APIOkRequestsExecutor.getExecutor().execute(requestBuilder.build(client));

        if(!response.isSuccess()){
            throw response.error;
        }
        if(!(response.results instanceof ReportExportResponse)){
            throw new RuntimeException("Not a String");
        }

        return (ReportExportResponse) response.results;

    }

    public int countAllBaseEntries(BaseEntryFilter filter) throws IOException {
        Client client = getClientInstance();
        BaseEntryService.CountBaseEntryBuilder count = BaseEntryService.count(filter);
        Response<Integer> response = (Response<Integer>) APIOkRequestsExecutor.getExecutor()
                .execute(count.build(client));
        return response.results;
    }

    public int countAllMediaEntries(MediaEntryFilter filter) throws IOException {
        Client client = getClientInstance();
        MediaService.CountMediaBuilder count = MediaService.count(filter);
        Response<Integer> response = (Response<Integer>) APIOkRequestsExecutor.getExecutor()
                .execute(count.build(client));
        return response.results;
    }

    public <T extends BaseEntryFilter> Set<? extends BaseEntry> listAllEntriesGeneric(T filter, BiFunction<T, FilterPager, BaseRequestBuilder> service) throws IOException {
        Client client = getClientInstance();
        FilterPager pager = new FilterPager();
        pager.setPageSize(BATCH_SIZE);
        pager.setPageIndex(1);

        int maxPages = MAX_RESULT_SIZE / BATCH_SIZE;
        Long lastCreatedTimeStamp = null;
        int count = 0;

        TreeSet<BaseEntry> allEntries = new TreeSet<>(Comparator.comparing(BaseEntry::getCreatedAt).thenComparing(BaseEntry::getId));

        while (true) {
            pager.setPageIndex(pager.getPageIndex());

            if (pager.getPageIndex() > maxPages) {
                filter.setCreatedAtGreaterThanOrEqual(lastCreatedTimeStamp);
                pager.setPageIndex(1);
            }

            BaseRequestBuilder listBuilder = service.apply(filter, pager);
            Response<ListResponse<BaseEntry>> response = null;
            try {
                response = (Response<ListResponse<BaseEntry>>) APIOkRequestsExecutor.getExecutor().execute(listBuilder.build(client));
            } catch (Exception e) {
                //TODO handle errors!
                log.info("Unexpected error", e);
            }

            if (response == null || response.isEmpty()) {
                break;
            }

            List<BaseEntry> result = response.results.getObjects();
            count += result.size();
            allEntries.addAll(result);
            lastCreatedTimeStamp = allEntries.last().getCreatedAt();

            log.info("Page: " + pager.getPageIndex());
            log.info("Response.size(): {}, total received: {}", result.size(), count);
            log.info("LatstCreatedTimeStamp: {}", lastCreatedTimeStamp);

            if (result.size() < pager.getPageSize()) {
                break;
            }
            pager.setPageIndex(pager.getPageIndex() + 1);
        }
        return allEntries;
    }

    public List<List<String>> getReportTable(ReportType reportType,  ReportInputFilter reportInputFilter,
                                             String order, String objectIds) throws IOException, APIException {
        //TODO: This reportedly only works up until 10000 total count even with paging. Testing indicates that it
        // gives duplicate rows when total count is above 10000.
        //TODO: Fix above (10000 limit), by ordering by created at and start new request from last entry. However the
        // it seems that the query needs to have a lower than 10000 total count.
        Client client = getClientInstance();
        FilterPager pager = new FilterPager();
        pager.setPageSize(BATCH_SIZE);

        StringBuilder rawData = new StringBuilder();
        int totalCount = BATCH_SIZE;
        int index = 0;

        ReportService.GetTableReportBuilder requestBuilder;
        Response<ReportTable> response;

        while(totalCount > BATCH_SIZE*index && 10000 > BATCH_SIZE*index) {
            if (BATCH_SIZE > totalCount - BATCH_SIZE * index){
                pager.setPageSize(totalCount - BATCH_SIZE * index);
            }
            index +=1;
            pager.setPageIndex(index);

            requestBuilder = ReportService.getTable(reportType, reportInputFilter,
                    pager, order, objectIds); //Build request
            response =
                    (Response<ReportTable>) APIOkRequestsExecutor.getExecutor()
                            .execute(requestBuilder.build(client));//Execute request and wait for response

            if(!response.isSuccess()){ //Check if request failed
                throw response.error;
            } else if (response.results.getData() == null) {
                return new ArrayList<>();
            }

            if(index == 1) { //Append header for first page
                rawData.append(response.results.getHeader()+";");
//                log.debug("Total Count: {}",response.results.getTotalCount());
            }
            totalCount = response.results.getTotalCount(); //Set total count
            rawData.append(response.results.getData()); //Append data from page
            log.debug("Page {} of getTable done with {} of {}", index, (index-1)*BATCH_SIZE+pager.getPageSize() ,
                    totalCount);
        }

        List<List<String>> rows = new ArrayList<>();
        Arrays.stream(rawData.toString().split(";")).forEach(x -> {
            List<String> col = Arrays.stream(x.split(",")).collect(Collectors.toList());
            rows.add(col);
        });

        return rows;
    }

    public List<List<String>> getReportTable(ReportType reportType,  ReportInputFilter reportInputFilter,
                                             String order) throws APIException, IOException {
        return getReportTable(reportType, reportInputFilter, order, null);
    }

}
