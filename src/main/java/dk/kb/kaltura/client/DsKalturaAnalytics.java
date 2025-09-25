package dk.kb.kaltura.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kaltura.client.enums.*;
import com.kaltura.client.services.BaseEntryService;
import com.kaltura.client.services.ESearchService;
import com.kaltura.client.services.MediaService;
import com.kaltura.client.services.ReportService;
import com.kaltura.client.types.*;
import com.kaltura.client.utils.request.BaseRequestBuilder;
import dk.kb.kaltura.domain.ReportTableDto;
import dk.kb.kaltura.domain.TopContentDto;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;


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

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename),
                StandardCharsets.UTF_8))) {
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
                    writer.write(mapper.writeValueAsString(mediaEntry));
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

    public List<BaseEntry> getEntriesFromIdList(List<String> objectIds) throws APIException {
        if (objectIds == null || objectIds.isEmpty()) {
            log.warn("Report from empty list will give unpredictable results on larger datasets. Returning empty map.");
            return new ArrayList<>();
        }
        if (objectIds.size() > MAX_RESULT_SIZE) {
            log.warn("This method is not designed to conserve memory and is not meant for larger datasets.");
        }

        int batchSize = getBatchSize();
        int remainingElements;
        int totalElements = objectIds.size();
        List<BaseEntry> results = new ArrayList<>();
        for (int i = 0; i < totalElements; i += batchSize) {
            remainingElements = totalElements - i;
            results.addAll(listEntryBatch(objectIds.subList(i, Math.min(batchSize + i, remainingElements + i))));
        }
        return results;
    }

    private List<BaseEntry> listEntryBatch(List<String> objectIds) throws APIException {
        if (objectIds.size() > getBatchSize()) {
            throw new IllegalArgumentException("Size of ObjectIds is greater than " + getBatchSize());
        }

        FilterPager pager = new FilterPager();
        pager.setPageSize(getBatchSize());
        ESearchEntryOperator entryOperator = new ESearchEntryOperator();
        entryOperator.setOperator(ESearchOperatorType.OR_OP);
        ESearchEntryParams searchParams = new ESearchEntryParams();

        searchParams.setObjectStatuses(EntryStatus.READY.getValue() + "," + EntryStatus.DELETED.getValue() +
                "," + EntryStatus.BLOCKED.getValue());

        entryOperator.setSearchItems(
                objectIds.stream()
                        .map(entryId -> {
                            var item = new ESearchEntryItem();
                            item.setFieldName(ESearchEntryFieldName.ID);
                            item.setItemType(ESearchItemType.EXACT_MATCH);
                            item.setSearchTerm(entryId);
                            return item;
                        })
                        .collect(Collectors.toList()));

        searchParams.setSearchOperator(entryOperator);

        List<BaseEntry> result = new ArrayList<>();
        ESearchEntryResponse response = handleRequest(ESearchService.searchEntry(searchParams, pager));

        response.getObjects()
                .stream()
                .map(ESearchEntryResult::getObject)
                .forEach(result::add);

        int resultSize = result.size();

        if (objectIds.size() != result.size()) {
            log.warn("Size of ObjectIds does not equal to final result size. {} != {}", objectIds.size(), resultSize);
        }
        log.debug("listMediaEntry returned {} results", resultSize);
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
        return new ReportTableDto(header, data.toString(), totalCount);
    }

    /**
     * Retrieves a list of top content based on the specified date range, domain, and object IDs.
     *
     * @param fromDay   The start date for the report in the format "yyyy-MM-dd".
     * @param toDay     The end date for the report in the format "yyyy-MM-dd".
     * @param domainIn  A string representing the domain filter for the report. Can be empty.
     * @param objectIds A list of object IDs to filter the report by. Must not be empty and must not exceed the
     *                  maximum result size: {@link #MAX_RESULT_SIZE}.
     * @return A list of {@link TopContentDto} containing the top content data for the specified parameters.
     * @throws APIException if an error occurs while calling the API to retrieve the report.
     */
    public List<TopContentDto> getTopContentFromIdList(String fromDay, String toDay, String domainIn,
                                                       List<String> objectIds) throws APIException {
        if (objectIds == null || objectIds.isEmpty()) {
            log.warn("Report from empty list will give unpredictable results on larger datasets. Returning empty map.");
            return new ArrayList<>();
        }
        if (objectIds.size() > MAX_RESULT_SIZE) {
            throw new IllegalArgumentException("Size of ObjectIds is greater than " + MAX_RESULT_SIZE);
        }

        ReportInputFilter reportInputFilter = new ReportInputFilter();
        reportInputFilter.setFromDay(fromDay);
        reportInputFilter.setToDay(toDay);
        if (domainIn == null || !domainIn.isEmpty()) {
            reportInputFilter.setDomainIn(domainIn);
        }

        StringBuilder objectIdStringBuilder = new StringBuilder();
        objectIds.forEach(objectId -> {
            objectIdStringBuilder.append(objectId.trim()).append(",");
        });

        ReportTableDto reportTableDto = getReportTable(ReportType.TOP_CONTENT, reportInputFilter,
                ReportOrderBy.CREATED_AT_ASC.getValue(), objectIdStringBuilder.toString());

        return reportTableTopContent(reportTableDto);
    }

    /**
     * Converts the data in the provided report table DTO into a list of top content DTOs.
     *
     * @param reportDto The {@link ReportTableDto} containing the report data to be processed.
     * @return A list of {@link TopContentDto} objects created from the report data.
     */
    private List<TopContentDto> reportTableTopContent(ReportTableDto reportDto) {
        List<TopContentDto> topContentDtos = new ArrayList<>();
        if (reportDto.getHeader().isEmpty()) {
            log.warn("Report map has an empty header");
            return topContentDtos;
        }

        for (String record : reportDto.getData().split(";")) { //TODO: Should this be handled in a more robust manner.
            String[] stringArr = record.split(",");
            topContentDtos.add(new TopContentDto(reportDto.getHeader(), stringArr));
        }

        return topContentDtos;

    }

}
