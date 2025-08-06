package dk.kb.kaltura.client;

import com.kaltura.client.Client;
import com.kaltura.client.enums.*;
import com.kaltura.client.services.*;
import com.kaltura.client.services.MediaService.*;
import com.kaltura.client.types.*;
import com.kaltura.client.utils.request.BaseRequestBuilder;
import com.kaltura.client.utils.response.base.Response;

import java.io.IOException;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;


public class DsKalturaAnalytics extends DsKalturaClientBase {
    final int MAX_RESULT_SIZE = 10000;

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
    public DsKalturaAnalytics(String kalturaUrl, String userId, int partnerId, String token, String tokenId, String adminSecret, int sessionDurationSeconds, int sessionRefreshThreshold) throws IOException {
        super(kalturaUrl, userId, partnerId, token, tokenId, adminSecret, sessionDurationSeconds, sessionRefreshThreshold);
    }

    public Map<String,List<List<String>>> getReportTableFromSegments(List<String> segments,
                                                                     ReportInputFilter reportInputFilter) throws APIException, IOException {

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

        return handleRequest(ReportService.getUrlForReportAsCsv(title, reportText, headers, reportType,
                reportInputFilter, dimension, pager, order, objectIds,  responseOptions));
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

        ReportExportParams exportParams = new ReportExportParams();
        exportParams.setRecipientEmail(email);
        exportParams.setBaseUrl(baseUrl);
        exportParams.setReportItems(reportExportItems);
        exportParams.setTimeZoneOffset(-120);

        return handleRequest(ReportService.exportToCsv(exportParams));
    }

    public int countAllBaseEntries(BaseEntryFilter filter) throws IOException, APIException {
        return handleRequest(BaseEntryService.count(filter));
    }

    public int countAllMediaEntries(MediaEntryFilter filter) throws IOException, APIException {
        return handleRequest(MediaService.count(filter));
    }

    public <T extends BaseEntryFilter> Set<? extends BaseEntry> listAllEntriesGeneric(T filter, BiFunction<T, FilterPager, BaseRequestBuilder> service) throws IOException {
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

            BaseRequestBuilder<ListResponse<BaseEntry>, ?> listBuilder = service.apply(filter, pager);
            ListResponse<BaseEntry> results;
            try {
                results = handleRequest(listBuilder);
            } catch (Exception e) {
                //TODO handle errors!
                log.info("Unexpected error", e);
                continue;
            }

            List<BaseEntry> result = results.getObjects();
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

            ReportTable results = handleRequest(ReportService.getTable(reportType, reportInputFilter,
                    pager, order, objectIds)); //Build request
            if (results.getData() == null) {
                return new ArrayList<>();
            }

            if(index == 1) { //Append header for first page
                rawData.append(results.getHeader()+";");
//                log.debug("Total Count: {}",response.results.getTotalCount());
            }
            totalCount = results.getTotalCount(); //Set total count
            rawData.append(results.getData()); //Append data from page
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
