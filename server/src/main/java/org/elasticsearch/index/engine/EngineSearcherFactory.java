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

package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;

import java.io.IOException;

/**
 * Basic Searcher factory that allows returning an {@link IndexSearcher}
 * given an {@link IndexReader}
 */
public class EngineSearcherFactory extends SearcherFactory { // NOTE: htt, 搜索引擎

    private final EngineConfig engineConfig; // NOTE: htt, 引擎配置

    public EngineSearcherFactory(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    @Override
    public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) throws IOException {
        IndexSearcher searcher = super.newSearcher(reader, previousReader);
        searcher.setQueryCache(engineConfig.getQueryCache()); // NOTE: htt, 设置查询缓存
        searcher.setQueryCachingPolicy(engineConfig.getQueryCachingPolicy()); // NOTE: htt, 设置查询缓存策略
        searcher.setSimilarity(engineConfig.getSimilarity()); // NOTE: htt, 相似度
        return searcher;
    }
}
