package org.elasticsearch.action.search;

import java.util.concurrent.ExecutionException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

public class SearchActionTimeoutIT extends ESIntegTestCase { // NOTE:htt, 查询超时测试

    private BulkRequest buildBulk(String index, String type, int numberOfRequest) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int j = 0; j < numberOfRequest; j++) {
            bulkRequest.add(new IndexRequest(index, type).source("timestamp", index));
        }
        return bulkRequest;
    }

    private SearchRequest buildSearchReq(String index, String type) {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        return searchRequest;
    }

    private Settings buildSearchTimeoutSettings(TimeValue timeValue) {
        return Settings.builder()
                .put(SearchSettings.SEARCH_RPC_TIMEOUT.getKey(), timeValue).build();
    }

    private Settings buildSearchNullSettings() {
        return Settings.builder()
                .putNull(SearchSettings.SEARCH_RPC_TIMEOUT.getKey()).build();
    }

    @After
    public void clearSearchSettings() {
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(buildSearchNullSettings())
                .setPersistentSettings(buildSearchNullSettings()).get();
    }

    public void updateSearchRPCTimeoutSettings(TimeValue timeValue) {
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(buildSearchTimeoutSettings(timeValue))
                .setPersistentSettings(buildSearchTimeoutSettings(timeValue)).get();
    }

    public void testRPCTimeoutForSeasrchAction() throws Exception {
        String indexName = "just_test_search";
        String type = "_doc";

        BulkResponse bulkResponses = client().bulk(buildBulk(indexName, type, 10000)).get();
        assertFalse(bulkResponses.hasFailures());

        // NOTE: no timeout
        clearSearchSettings();
        updateSearchRPCTimeoutSettings(TimeValue.ZERO);
        SearchResponse searchResponse = client().search(buildSearchReq(indexName, type)).get();
        assertEquals(0, searchResponse.getFailedShards());

        try {
            // NOTE:htt, set short timeout for search
            updateSearchRPCTimeoutSettings(TimeValue.timeValueMillis(1));
            logger.info("htt: set 1 millisecond timeout for search");

            searchResponse = client().search(buildSearchReq(indexName, type)).get(); // NOTE:htt, 查询出现超时
            assertFalse(searchResponse.isTimedOut());
            logger.info("htt: response: " + searchResponse);
            assertTrue(searchResponse.getFailedShards() > 0); // NOTE:htt, 可能部分请求成功，部分失败
            assertTrue(searchResponse.getShardFailures()[0].getCause().getMessage().contains("timed out after"));
        } catch (ExecutionException e) {
            logger.info("htt: exception: " + e.getMessage());
            assertTrue(e.getMessage().contains("all shards failed")); // NOTE:htt, 可能查询时全部失败，即在query阶段全部失败
        }
    }
}
