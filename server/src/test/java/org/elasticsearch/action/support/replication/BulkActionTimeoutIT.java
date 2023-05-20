/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

public class BulkActionTimeoutIT extends ESIntegTestCase {

    private BulkRequest buildBulk(String index, String type, int numberOfRequest) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int j = 0; j < numberOfRequest; j++) {
            bulkRequest.add(new IndexRequest(index, type).source("timestamp", index));
        }
        return bulkRequest;
    }

    private Settings buildBulkTimeoutSettings(TimeValue timeValue) {
        return Settings.builder()
                .put(IndicesService.BULK_RPC_TIMEOUT.getKey(), timeValue)
                .build();
    }

    private Settings buildBulkNullSettings() {
        return Settings.builder()
                .putNull(IndicesService.BULK_RPC_TIMEOUT.getKey())
                .build();
    }

    @After
    public void clearBulkSettings() {
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(buildBulkNullSettings())
                .setPersistentSettings(buildBulkNullSettings()).get();
    }

    public void updateBulkRPCTimeoutSettings(TimeValue timeValue) {
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(buildBulkTimeoutSettings(timeValue))
                .setPersistentSettings(buildBulkTimeoutSettings(timeValue)).get();
    }

    public void testRPCTimeoutForBulkAction() throws Exception {
        String indexName = "just_test_bulk";
        String type = "_doc";

        // NOTE: no timeout
        clearBulkSettings();
        updateBulkRPCTimeoutSettings(TimeValue.ZERO);
        BulkResponse bulkResponses = client().bulk(buildBulk(indexName, type, 10000)).get();
        assertFalse(bulkResponses.hasFailures());

        // NOTE: set short timeout for bulk request
        updateBulkRPCTimeoutSettings(TimeValue.timeValueMillis(1));
        bulkResponses = client().bulk(buildBulk(indexName, type, 10000)).get();
        assertTrue(bulkResponses.hasFailures());
        assertTrue(bulkResponses.getItems()[0].getFailureMessage().contains("timed out after"));
        SearchResponse preSearchResponse = client().prepareSearch(indexName).get();
        assertFalse(preSearchResponse.isTimedOut());
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(indexName).get();
        assertEquals(0, refreshResponse.getFailedShards());

        // NOTE: set long timeout for bulk request
        updateBulkRPCTimeoutSettings(TimeValue.timeValueMillis(30000));
        bulkResponses = client().bulk(buildBulk(indexName, type, 10000)).get();
        assertFalse(bulkResponses.hasFailures());
        preSearchResponse = client().prepareSearch(indexName).get();
        assertFalse(preSearchResponse.isTimedOut());
        refreshResponse = client().admin().indices().prepareRefresh(indexName).get();
        assertEquals(0, refreshResponse.getFailedShards());
    }

}
