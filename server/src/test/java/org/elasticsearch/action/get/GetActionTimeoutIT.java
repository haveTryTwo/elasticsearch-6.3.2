package org.elasticsearch.action.get;

import java.util.concurrent.ExecutionException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

public class GetActionTimeoutIT extends ESIntegTestCase { // NOTE:htt, 查询超时测试

    private BulkRequest buildBulk(String index, String type, int numberOfRequest) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int j = 0; j < numberOfRequest; j++) {
            bulkRequest.add(new IndexRequest(index, type, String.valueOf(j)).source("timestamp", index));
        }
        return bulkRequest;
    }

    private GetRequest buildGetReq(String index, String type, String id) {
        GetRequest getRequest = new GetRequest(index, type, id);
        return getRequest;
    }

    private Settings buildGetTimeoutSettings(TimeValue timeValue) {
        return Settings.builder()
                .put(IndicesService.GET_RPC_TIMEOUT.getKey(), timeValue).build();
    }

    private Settings buildGetNullSettings() {
        return Settings.builder()
                .putNull(IndicesService.GET_RPC_TIMEOUT.getKey()).build();
    }

    @After
    public void clearGetSettings() {
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(buildGetNullSettings())
                .setPersistentSettings(buildGetNullSettings()).get();
    }

    public void updateGetRPCTimeoutSettings(TimeValue timeValue) {
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(buildGetTimeoutSettings(timeValue))
                .setPersistentSettings(buildGetTimeoutSettings(timeValue)).get();
    }

    public void testRPCTimeoutForSeasrchAction() throws Exception {
        String indexName = "just_test_get";
        String type = "_doc";

        BulkResponse bulkResponses = client().bulk(buildBulk(indexName, type, 10000)).get();
        assertFalse(bulkResponses.hasFailures());

        // NOTE: no timeout
        clearGetSettings();
        updateGetRPCTimeoutSettings(TimeValue.ZERO);
        GetResponse getResponse = client().get(buildGetReq(indexName, type, "0")).get();
        assertEquals(true, getResponse.isExists());
        logger.info("htt get reponse: " + getResponse.toString());

        try {
            // NOTE:htt, set short timeout for get
            updateGetRPCTimeoutSettings(TimeValue.timeValueMillis(1));
            logger.info("htt: set 1 millisecond timeout for get");

            getResponse = client().get(buildGetReq(indexName, type, "11")).get(); // NOTE:htt, 查询出现超时
            logger.info("htt get reponse: " + getResponse.toString());
        } catch (ExecutionException e) {
            logger.info("htt exception: " + e.getMessage());
        }

        // NOTE:htt, set long timeout for get
        updateGetRPCTimeoutSettings(TimeValue.timeValueMillis(30000));
        logger.info("htt: set 30000 millisecond timeout for get");

        getResponse = client().get(buildGetReq(indexName, type, "22")).get(); // NOTE:htt, 查询正常
        assertEquals(true, getResponse.isExists());
        logger.info("htt get reponse: " + getResponse.toString());

        getResponse = client().get(buildGetReq(indexName, type, "1111122")).get(); // NOTE:htt, 查询正常
        assertEquals(false, getResponse.isExists());
        logger.info("htt get reponse: " + getResponse.toString());
    }
}
