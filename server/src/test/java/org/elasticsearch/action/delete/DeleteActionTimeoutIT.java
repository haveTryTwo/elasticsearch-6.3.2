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


package org.elasticsearch.action.delete;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

public class DeleteActionTimeoutIT extends ESIntegTestCase {

    private BulkRequest buildBulk(String index, String type, int numberOfRequest) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int j = 0; j < numberOfRequest; j++) {
            bulkRequest.add(new IndexRequest(index, type, String.valueOf(j)).source("timestamp", index));
        }
        return bulkRequest;
    }

    private DeleteRequest buildDelete(String index, String type, String id) {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);

        return deleteRequest;
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
        String indexName = "just_test_delete";
        String type = "_doc";

        // NOTE: no timeout
        clearBulkSettings();
        updateBulkRPCTimeoutSettings(TimeValue.ZERO);
        BulkResponse bulkResponses = client().bulk(buildBulk(indexName, type, 10000)).get();
        assertFalse(bulkResponses.hasFailures());

        DeleteResponse deleteResponse = client().delete(buildDelete(indexName, type, "0")).get();
        System.out.println("delete reponse: " + deleteResponse.toString());
        assertTrue(deleteResponse.getShardInfo().getFailed() == 0);

        // NOTE: set short timeout for delete request
        updateBulkRPCTimeoutSettings(TimeValue.timeValueMillis(1));
        try {
            deleteResponse = client().delete(buildDelete(indexName, type, "11")).get();
            System.out.println("delete reponse: " + deleteResponse.toString());
        } catch (Exception e) {
            System.out.println("exception: " + e.getMessage());
        }

        // NOTE: set long timeout for delete request
        updateBulkRPCTimeoutSettings(TimeValue.timeValueMillis(30000));
        deleteResponse = client().delete(buildDelete(indexName, type, "111")).get();
        assertTrue(deleteResponse.getShardInfo().getFailed() == 0);
    }

}
