package dk.kb.kaltura.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.kaltura.client.enums.*;
import com.kaltura.client.services.BaseEntryService;
import com.kaltura.client.services.ESearchService;
import com.kaltura.client.services.MediaService;
import com.kaltura.client.services.ReportService;
import com.kaltura.client.types.*;
import com.kaltura.client.utils.request.BaseRequestBuilder;
import dk.kb.kaltura.domain.ReportTableDto;
import dk.kb.kaltura.domain.TopContentDto;
import dk.kb.kaltura.mapper.TopContentDtoMapper;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class DsKalturaAnalytics extends DsKalturaClientBase {

    private final TopContentDtoMapper topContentDtoMapper;

    /**
     * This Client fetches analytics data from Kaltura. Either a token/tokenId a adminSecret must be provided
     * for authentication in order to start a session. Session will be reused between Kaltura calls
     * without authenticating again.
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
     *
     */
    public DsKalturaAnalytics(String kalturaUrl, String userId, int partnerId, String token, String tokenId, String adminSecret, int sessionDurationSeconds, int sessionRefreshThreshold) throws APIException {
        super(kalturaUrl, userId, partnerId, token, tokenId, adminSecret, sessionDurationSeconds,
                sessionRefreshThreshold, MAX_BATCH_SIZE);
        this.topContentDtoMapper = new TopContentDtoMapper();
    }

    public int countAllBaseEntries(BaseEntryFilter filter) throws APIException {
        return handleRequest(BaseEntryService.count(filter));
    }

    public int countAllMediaEntries(MediaEntryFilter filter) throws APIException {
        return handleRequest(MediaService.count(filter));
    }

    /**
     * Exports all entries of a specified type to a file, applying a given filter and service for pagination.
     * This method retrieves entries in batches based on the provided filter and writes them
     * to a specified file in JSON format. It handles pagination and ensures no duplicate entries
     * are written to the file. The entries are ordered by their creation timestamp.
     *
     * @param <T>      The type of filter used to specify the criteria for entries to export.
     * @param <E>      The type of entries being exported, extending from BaseEntry.
     * @param <B>      The type of request builder used to create service requests, extending from BaseRequestBuilder.
     * @param filter   The filter to apply for exporting entries.
     * @param service  A function that takes the filter and a FilterPager, and returns a configured request builder.
     * @param filename The name of the file to which the entries will be exported.
     * @throws RuntimeException if there is an API exception while handling the request.
     */
    public <T extends BaseEntryFilter, E extends BaseEntry, B extends BaseRequestBuilder<ListResponse<E>, B>> void
    exportAllEntriesToFile(T filter, BiFunction<T, FilterPager, B> service, String filename) {
        FilterPager filterPager = new FilterPager();
        filterPager.setPageSize(getBatchSize());
        filterPager.setPageIndex(1);
        filter.setOrderBy(MediaEntryOrderBy.CREATED_AT_ASC.getValue());

        Long lastCreatedTimestamp = 1L;
        int count = 0;
        List<E> result;
        ObjectMapper mapper = new ObjectMapper();
        Set<String> lastPage = Sets.newHashSet();

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename),
                StandardCharsets.UTF_8))) {
            while (true) {
                filter.setCreatedAtGreaterThanOrEqual(lastCreatedTimestamp);

                B listBuilder = service.apply(filter, filterPager);

                result = handleRequest(listBuilder).getObjects();

                for (E mediaEntry : result) {
                    if (lastPage.contains(mediaEntry.getId())) {
                        continue;
                    }
                    count += 1;
                    lastCreatedTimestamp = mediaEntry.getCreatedAt();
                    writer.write(mapper.writeValueAsString(mediaEntry));
                    writer.newLine();
                }
                writer.flush();

                log.info("Page: {}, result.size(): {}, total received: {}", filterPager.getPageIndex(), result.size(),
                        count);
                log.debug("LastCreatedTimestamp: {}", lastCreatedTimestamp);

                if (result.size() < getBatchSize()) {
                    log.info("No more entries found");
                    break;
                }
                lastPage = result.stream().map(E::getId).collect(Collectors.toSet());
            }

        } catch (IOException e) {
            throw new RuntimeException("IOExeption while writing to file: ", e);
        } catch (APIException e) {
            throw new RuntimeException("APIException while handling request: ", e);
        }
    }

    /**
     * Retrieves a list of BaseEntry objects corresponding to the provided list of object IDs.
     * This method processes the provided list of object IDs in batches to retrieve entries.
     * If the input list is null or empty, it logs a warning and returns an empty list. If the
     * input list exceeds a predefined maximum size, a warning is also logged. The method uses
     * batching to efficiently retrieve entries in smaller groups.
     *
     * @param objectIds A list of object IDs for which the corresponding BaseEntry objects are to be retrieved.
     *                  If this list is null or empty, an empty list is returned.
     * @return A list of BaseEntry objects corresponding to the provided object IDs. If no valid IDs are
     * provided, an empty list is returned.
     * @throws APIException If there is an error while retrieving the entries from the service.
     */
    public List<BaseEntry> getEntriesFromIdList(List<String> objectIds) throws APIException {
        if (objectIds == null || objectIds.isEmpty()) {
            throw new IllegalArgumentException("Null or empty objectIds list");
        }
        if (objectIds.size() > MAX_RESULT_SIZE) {
            log.warn("This method is not designed to conserve memory and is not meant for larger datasets.");
        }

        int batchSize = getBatchSize();
        int totalElements = objectIds.size();
        List<BaseEntry> results = new ArrayList<>();
        for (int i = 0; i < totalElements; i += batchSize) {
            results.addAll(listEntryBatch(objectIds.subList(i, Math.min(batchSize + i, totalElements))));
        }
        return results;
    }

    /**
     * Retrieves a batch of MediaEntry objects based on the provided list of object IDs.
     * This method checks if the number of provided object IDs exceeds the defined batch size.
     * If it does, an IllegalArgumentException is thrown. It constructs a search query
     * to fetch the corresponding MediaEntry objects from the backend service and logs any
     * missing IDs or discrepancies in the expected versus actual results.
     *
     * @param objectIds A list of object IDs to retrieve MediaEntry objects for.
     *                  The size of this list must not exceed the configured batch size.
     * @return A list of MediaEntry objects corresponding to the provided object IDs.
     * @throws APIException             If there is an error while handling the request to the search service.
     * @throws IllegalArgumentException If the size of the objectIds list exceeds the configured batch size.
     */
    private List<MediaEntry> listEntryBatch(List<String> objectIds) throws APIException {
        if (objectIds.size() > getBatchSize()) {
            throw new IllegalArgumentException("Size of objectIds: " + objectIds.size() + " is greater than batchSize: " + getBatchSize());
        }

        FilterPager filterPager = new FilterPager();
        filterPager.setPageSize(getBatchSize());
        ESearchEntryOperator entryOperator = new ESearchEntryOperator();
        entryOperator.setOperator(ESearchOperatorType.OR_OP);
        ESearchEntryParams searchParams = new ESearchEntryParams();

        entryOperator.setSearchItems(
                objectIds.stream()
                        .map(entryId -> {
                            ESearchEntryItem item = new ESearchEntryItem();
                            item.setFieldName(ESearchEntryFieldName.ID);
                            item.setItemType(ESearchItemType.EXACT_MATCH);
                            item.setSearchTerm(entryId);
                            return item;
                        })
                        .collect(Collectors.toList()));

        searchParams.setSearchOperator(entryOperator);

        List<MediaEntry> result = new ArrayList<>();
        ESearchEntryResponse response = handleRequest(ESearchService.searchEntry(searchParams, filterPager));

        response.getObjects()
                .stream()
                .map(ESearchEntryResult::getObject)
                .map(entry -> (MediaEntry) entry)
                .forEach(result::add);

        int resultSize = result.size();

        Set<String> resultIdSet = result.stream().map(BaseEntry::getId).collect(Collectors.toSet());
        for (String objectId : objectIds) {
            if (!resultIdSet.contains(objectId)) {
                log.warn("kaltura id missing: {}", objectId);
            }
        }
        if (objectIds.size() != resultIdSet.size()) {
            log.warn("Requested ({}) != result size({})", objectIds.size(), resultSize);
        } else {
            log.debug("Requested ({}) == result size({})", objectIds.size(), resultIdSet.size());
        }

        return result;
    }

    /**
     * Retrieves a report table based on the specified report type and input filter.
     * The method paginates through the results to accommodate large datasets and
     * compiles the report data into a single map containing the header, total count,
     * and data of the report. This method is limited to only fetch {@link #MAX_RESULT_SIZE} results
     * to not exceed Kalturas documented limits on API service.
     *
     * @param reportType        The type of report to generate, specified by the {@link ReportType} enum.
     * @param reportInputFilter The filter to apply to the report's input data, specified by {@link ReportInputFilter}.
     * @param order             The order in which to sort the report data, typically a string representation of sorting criteria.
     * @param objectIds         A string containing the IDs of specific objects to include in the report.
     * @return A map containing the report's header, total count of records, and the data as a string.
     * The map has the following keys:
     * <ul>
     *     <li><b>header</b>: A string representing the report's header.</li>
     *     <li><b>totalCount</b>: A string representation of the total number of records in the report.</li>
     *     <li><b>data</b>: A string containing the data of the report.</li>
     * </ul><
     * @throws APIException If an error occurs while retrieving the report, such as issues with the API request.
     */
    public ReportTableDto getReportTable(ReportType reportType, ReportInputFilter reportInputFilter,
                                         String order, String objectIds) throws APIException {
        FilterPager filterPager = new FilterPager();
        filterPager.setPageSize(getBatchSize());

        StringBuilder stringBuilderData = new StringBuilder();
        int totalCount = getBatchSize();
        int index = 0;
        String header = "";
        while (totalCount > getBatchSize() * index && MAX_RESULT_SIZE > getBatchSize() * index) {
            index++;
            filterPager.setPageIndex(index);
            ReportService.GetTableReportBuilder requestBuilder = ReportService.getTable(reportType, reportInputFilter,
                    filterPager, order, objectIds);

            ReportTable results = handleRequest(requestBuilder);

            if (results.getData() == null) {
                totalCount = 0;
                break;
            }
            stringBuilderData.append(results.getData());
            totalCount = results.getTotalCount();
            header = results.getHeader();
        }
        return new ReportTableDto(header, stringBuilderData.toString(), totalCount);
    }

    /**
     * Retrieves a list of  {@link TopContentDto} based on the specified date range, domain, and objectIDs.
     *
     * @param fromDay   The start date for the report in the format "yyyyMMdd".
     * @param toDay     The end date for the report in the format "yyyyMMdd".
     * @param domainIn  A string representing the domain filter for the report. Can be empty.
     * @param objectIds A list of object IDs to filter the report by. Must not be empty and must not exceed the
     *                  maximum result size: {@link #MAX_RESULT_SIZE}.
     * @return A list of {@link TopContentDto} containing the top content data for the specified parameters.
     * @throws APIException if an error occurs while calling the API to retrieve the report.
     */
    public List<TopContentDto> getTopContentFromIdList(LocalDate fromDay, LocalDate toDay, String domainIn,
                                                       List<String> objectIds) throws APIException, IOException {
        if (objectIds == null || objectIds.isEmpty()) {
            throw new IllegalArgumentException("objectIds is empty or null");
        }
        if (objectIds.size() > MAX_RESULT_SIZE) {
            throw new IllegalArgumentException("Size of objectIds: " + objectIds.size() + " is greater than " +
                    "MAX_RESULT_SIZE: " + MAX_RESULT_SIZE);
        }

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.getDefault());
        String formattedFromDate = fromDay.format(dateTimeFormatter);
        String formattedToDate = toDay.format(dateTimeFormatter);

        ReportInputFilter reportInputFilter = new ReportInputFilter();
        reportInputFilter.setFromDay(formattedFromDate);
        reportInputFilter.setToDay(formattedToDate);
        if (domainIn != null && !domainIn.isEmpty()) {
            reportInputFilter.setDomainIn(domainIn);
        }

        String objectIdString = objectIds.stream()
                .map(String::trim)
                .collect(Collectors.joining(","));

        ReportTableDto reportTableDto = getReportTable(ReportType.TOP_CONTENT, reportInputFilter,
                ReportOrderBy.CREATED_AT_ASC.getValue(), objectIdString);
        List<TopContentDto> result = topContentDtoMapper.map(reportTableDto);
        log.info("Size of TopContent Report: {}", result.size());
        return result;
    }
}
