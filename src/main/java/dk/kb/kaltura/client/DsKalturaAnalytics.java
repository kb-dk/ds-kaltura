package dk.kb.kaltura.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.kaltura.client.enums.*;
import com.kaltura.client.services.*;
import com.kaltura.client.types.*;
import com.kaltura.client.utils.request.BaseRequestBuilder;
import org.yaml.snakeyaml.reader.StreamReader;

import java.io.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;


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

        //TODO: BUGFIX - This creates duplicates entries.
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
                for (BaseEntry mediaEntry : result) {
                    if (lastEntry.getId().equals(mediaEntry.getId())) {
                        continue;
                    }
                    String json = new Gson().toJson(mediaEntry);
                    writer.write(json);
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

    public Map<String, String> getReportTable(ReportType reportType, ReportInputFilter reportInputFilter,
                                              String order, String objectIds) throws APIException {
        FilterPager pager = new FilterPager();
        pager.setPageSize(getBatchSize());

        StringBuilder rawData = new StringBuilder();
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
            rawData.append(results.getData());
            totalCount = results.getTotalCount();
            header = results.getHeader();
        }
        return Map.of("header", header, "totalCount", String.valueOf(totalCount), "data", rawData.toString());
    }

    public void reportFromJson(String inputFilePath, String outputFilePath,
                               ReportInputFilter reportInputFilter) throws IOException, APIException {
        log.info("Starting to report from {} to {}", inputFilePath, outputFilePath);
        ObjectMapper objectMapper = new ObjectMapper();

        try (FileWriter fileWriter = new FileWriter(outputFilePath)) {
            Stream<String> lines = new BufferedReader(new FileReader(inputFilePath))
                    .lines()
                    .map(line -> {
                        try {
                            return objectMapper.readValue(line, Map.class).get(
                                    "id").toString();
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    });

            reportFromIds(lines, fileWriter, reportInputFilter);
        }
    }

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
