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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequestOptions;

public class BulkAction extends Action<BulkRequest, BulkResponse, BulkRequestBuilder> {

    public static final BulkAction INSTANCE = new BulkAction();
    public static final String NAME = "indices:data/write/bulk";

    private BulkAction() {
        super(NAME);
    }

    @Override
    public BulkResponse newResponse() {
        return new BulkResponse();
    }

    @Override
    public BulkRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new BulkRequestBuilder(client, this);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) { // NOTE:htt, bulk写入的options设置
        return TransportRequestOptions.builder()
                .withType(TransportRequestOptions.Type.BULK)
                .withCompress(settings.getAsBoolean("action.bulk.compress", true)
                ).build();
    }
}
