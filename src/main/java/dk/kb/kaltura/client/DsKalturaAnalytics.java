package dk.kb.kaltura.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kaltura.client.enums.ReportOrderBy;
import com.kaltura.client.enums.ReportType;
import com.kaltura.client.services.BaseEntryService;
import com.kaltura.client.services.MediaService;
import com.kaltura.client.services.ReportService;
import com.kaltura.client.types.*;
import com.kaltura.client.utils.request.BaseRequestBuilder;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;


public class DsKalturaAnalytics extends DsKalturaClientBase {

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
    public DsKalturaAnalytics(String kalturaUrl, String userId, int partnerId, String token, String tokenId, String adminSecret, int sessionDurationSeconds, int sessionRefreshThreshold) throws APIException {
        super(kalturaUrl, userId, partnerId, token, tokenId, adminSecret, sessionDurationSeconds,
                sessionRefreshThreshold, MAX_BATCH_SIZE);
    }

    public int countAllBaseEntries(BaseEntryFilter filter) throws APIException {
        return handleRequest(BaseEntryService.count(filter));
    }

    public int countAllMediaEntries(MediaEntryFilter filter) throws APIException {
        return handleRequest(MediaService.count(filter));
    }

    @SuppressWarnings("unchecked")
    public <T extends BaseEntryFilter, E extends BaseEntry, B extends BaseRequestBuilder<ListResponse<E>, B>> void
    exportAllEntriesToFile(T filter, BiFunction<T, FilterPager, B> service, String filename) {

        //TODO: BUGFIX - This creates duplicates entries on larger datasets.
        FilterPager pager = new FilterPager();
        pager.setPageSize(getBatchSize());
        pager.setPageIndex(1);
        filter.setOrderBy("createdAt_ASC");

        int maxPages = MAX_RESULT_SIZE / getBatchSize();
        Long lastCreatedTimeStamp = null;
        BaseEntry lastEntry;
        int count = 0;
        List<BaseEntry> result;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            while (true) {
                pager.setPageIndex(pager.getPageIndex());

                if (pager.getPageIndex() > maxPages) {
                    filter.setCreatedAtGreaterThanOrEqual(lastCreatedTimeStamp);
                    pager.setPageIndex(1);
                }

                B listBuilder = service.apply(filter, pager);

                result = (List<BaseEntry>) handleRequest(listBuilder).getObjects();
                count += result.size();
                lastEntry = result.get(result.size() - 1);
                lastCreatedTimeStamp = lastEntry.getCreatedAt();

                log.info("Page: " + pager.getPageIndex());
                log.info("Response.size(): {}, total received: {}", result.size(), count);
                log.info("LatstCreatedTimeStamp: {}", lastCreatedTimeStamp);

                ObjectMapper mapper = new ObjectMapper();
                for (BaseEntry mediaEntry : result) {
                    if (lastEntry.getId().equals(mediaEntry.getId())) {
                        continue;
                    }
                    mapper.writeValue(writer, mediaEntry);
                    writer.newLine();

                }
                writer.flush();

                if (result.size() < pager.getPageSize()) {
                    break;
                }
                pager.setPageIndex(pager.getPageIndex() + 1);
            }

        } catch (IOException e) {
            //TODO handle exception
            e.printStackTrace();
        } catch (APIException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves a report table based on the specified report type and input filter.
     * The method paginates through the results to accommodate large datasets and
     * compiles the report data into a single map containing the header, total count,
     * and data of the report. This method is limited to only fetch {@link #MAX_RESULT_SIZE} results
     * to not exceed Kalturas documented limits on API service.
     *
     * @param reportType       The type of report to generate, specified by the {@link ReportType} enum.
     * @param reportInputFilter The filter to apply to the report's input data, specified by {@link ReportInputFilter}.
     * @param order            The order in which to sort the report data, typically a string representation of sorting criteria.
     * @param objectIds        A string containing the IDs of specific objects to include in the report.
     * @return A map containing the report's header, total count of records, and the data as a string.
     *         The map has the following keys:
     *         <ul>
     *             <li><b>header</b>: A string representing the report's header.</li>
     *             <li><b>totalCount</b>: A string representation of the total number of records in the report.</li>
     *             <li><b>data</b>: A string containing the data of the report.</li>
     *         </ul>
     * @throws APIException If an error occurs while retrieving the report, such as issues with the API request.
     */
    public Map<String, String> getReportTable(ReportType reportType, ReportInputFilter reportInputFilter,
                                              String order, String objectIds) throws APIException {
        FilterPager pager = new FilterPager();
        pager.setPageSize(getBatchSize());

        StringBuilder data = new StringBuilder();
        int totalCount = getBatchSize();
        int index = 0;
        String header = "";
        while (totalCount > getBatchSize() * index && MAX_RESULT_SIZE > getBatchSize() * index) {
            totalCount = 0;
            index++;
            pager.setPageIndex(index);
            ReportService.GetTableReportBuilder requestBuilder = ReportService.getTable(reportType, reportInputFilter,
                    pager, order, objectIds);

            ReportTable results = handleRequest(requestBuilder);

            if (results.getData() == null) {
                break;
            }
            data.append(results.getData());
            totalCount = results.getTotalCount();
            header = results.getHeader();
        }
        return Map.of("header", header, "totalCount", String.valueOf(totalCount), "data", data.toString());
    }

    /**
     * Generates a report from a stream of object IDs and writes the output to the specified writer.
     * This method processes the IDs in batches and utilizes the provided {@link ReportInputFilter}
     * to filter the report data. It manages the writing of results to the output writer efficiently,
     * flushing the content at the end of the operation.
     *
     * <p>The method logs the progress of the reporting process, including the counts of queried
     * items and the response items received from each batch. If the number of queried items reaches
     * the {@code MAX_RESULT_SIZE}, it triggers the processing of a batch of data.</p>
     *
     * @param inputStream      A stream of object IDs to be reported, represented as strings.
     * @param outputWriter     A {@link Writer} instance to which the report data will be written.
     * @param reportInputFilter The filter to apply to the report's input data, specified by {@link ReportInputFilter}.
     * @throws IOException If an error occurs while writing to the {@code Writer} or processing the stream.
     * @throws APIException If an error occurs while retrieving the report, such as issues with the API request.
     */
    public void reportFromIds(Stream<String> inputStream, Writer outputWriter,
                              ReportInputFilter reportInputFilter) throws IOException, APIException {
        log.info("Starting to report from objectIds");
        try (BufferedWriter bw = new BufferedWriter(outputWriter)) {
            int queriedItemCount = 1;
            StringBuilder sb = new StringBuilder();
            int resultItemCount = 0;
            Iterator<String> streamIterator = inputStream.iterator();
            while (streamIterator.hasNext()) {
                sb.append(streamIterator.next());
                sb.append(",");
                if (queriedItemCount % MAX_RESULT_SIZE == 0) {
                    resultItemCount += processBatch(reportInputFilter, sb.toString(), bw, resultItemCount);
                    log.info("QueryCount {}, reponseCount {}", queriedItemCount, resultItemCount);
                    sb.setLength(0);
                }
                queriedItemCount++;
            }
            if (sb.length() > 0) {
                resultItemCount += processBatch(reportInputFilter, sb.toString(), bw, resultItemCount);
                log.info("QueryCount {}, reponseCount {}", queriedItemCount, resultItemCount);
            }
            bw.flush();
        }
        log.info("Finished report from objectIds");
    }

    /**
     * Processes a batch of report data based on the provided input filter and object IDs.
     * This method retrieves a report table, writes the header and data to a provided
     * {@link BufferedWriter}, and returns the total count of items in the report.
     *
     * <p>If no items have been queried yet and the header is present in the retrieved report,
     * the header will be written to the {@code BufferedWriter} first. The data from the report will
     * replace semicolons with the system's line separator before being written.</p>
     *
     * @param reportInputFilter The filter to apply to the report's input data, specified by {@link ReportInputFilter}.
     * @param objectIds        A string containing the IDs of specific objects to include in the report.
     * @param bw               A {@link BufferedWriter} to which the report header and data will be written.
     * @param queriedItemCount The count of items that have been queried so far; used to determine if the header should be written.
     * @return The total count of items in the report, as an integer.
     * @throws IOException If an error occurs while writing to the {@code BufferedWriter}.
     * @throws APIException If an error occurs while retrieving the report, such as issues with the API request.
     */
    private int processBatch(ReportInputFilter reportInputFilter, String objectIds, BufferedWriter bw, int queriedItemCount) throws IOException, APIException {
        Map<String, String> map = getReportTable(ReportType.TOP_CONTENT, reportInputFilter,
                ReportOrderBy.CREATED_AT_ASC.getValue(), objectIds);
        if (queriedItemCount == 0 && !map.get("header").isEmpty()) {
            bw.write(map.get("header"));
            bw.newLine();
        }
        bw.write(map.get("data").replace(";", System.lineSeparator()));
        return Integer.parseInt(map.get("totalCount"));
    }

}
