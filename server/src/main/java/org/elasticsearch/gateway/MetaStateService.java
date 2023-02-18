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

package org.elasticsearch.gateway;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Handles writing and loading both {@link MetaData} and {@link IndexMetaData}
 */
public class MetaStateService extends AbstractComponent { // NOTE: htt, 处理 节点元信息、索引元信息的加载和写入

    private final NodeEnvironment nodeEnv; // NOTE: htt, node enviroment including nodeId/NodePath[](node_path/index_path)
    private final NamedXContentRegistry namedXContentRegistry;

    public MetaStateService(Settings settings, NodeEnvironment nodeEnv, NamedXContentRegistry namedXContentRegistry) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    /**
     * Loads the full state, which includes both the global state and all the indices
     * meta state.
     */
    MetaData loadFullState() throws IOException { // NOTE: htt, 加载节点集群元信息，索引元信息
        MetaData globalMetaData = loadGlobalState(); // NOTE: htt, 加载 ${data.paths}/nodes/0/_state/global-xxx.st 文件
        MetaData.Builder metaDataBuilder;
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
        } else {
            metaDataBuilder = MetaData.builder();
        }
        for (String indexFolderName : nodeEnv.availableIndexFolders()) { // NOTE: htt, 索引元信息, ${data.paths}/nodes/0/indices/${index.UUID}/_state/state-xxx.st
            IndexMetaData indexMetaData = IndexMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry,
                nodeEnv.resolveIndexFolder(indexFolderName)); // NOTE:htt, 本地读取索元信息
            if (indexMetaData != null) {
                metaDataBuilder.put(indexMetaData, false);
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return metaDataBuilder.build();
    }

    /**
     * Loads the index state for the provided index name, returning null if doesn't exists.
     */
    @Nullable
    public IndexMetaData loadIndexState(Index index) throws IOException { // NOTE: htt, 加载索引的元信息
        return IndexMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.indexPaths(index));
    }

    /**
     * Loads all indices states available on disk
     */
    List<IndexMetaData> loadIndicesStates(Predicate<String> excludeIndexPathIdsPredicate) throws IOException { // NOTE: htt, 加载节点下所有路径的元信息，排除的索引不考虑）
        List<IndexMetaData> indexMetaDataList = new ArrayList<>();
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            if (excludeIndexPathIdsPredicate.test(indexFolderName)) { // NOTE: htt, 如果需要排除，则不考虑对应索引，excludeIndexPathIdsPredicate为UUID
                continue;
            }
            IndexMetaData indexMetaData = IndexMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry,
                nodeEnv.resolveIndexFolder(indexFolderName)); // NOTE: htt, 加载索引的元信息
            if (indexMetaData != null) {
                final String indexPathId = indexMetaData.getIndex().getUUID();
                if (indexFolderName.equals(indexPathId)) { // NOTE: htt, 磁盘上存储的索引的UUID 必须和 索引元信息中(state-xx.st)中的UUID一致
                    indexMetaDataList.add(indexMetaData);
                } else {
                    throw new IllegalStateException("[" + indexFolderName+ "] invalid index folder name, rename to [" + indexPathId + "]");
                }
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return indexMetaDataList;
    }

    /**
     * Loads the global state, *without* index state, see {@link #loadFullState()} for that.
     */
    MetaData loadGlobalState() throws IOException { // NOTE: htt, 加载 ${data.paths}/nodes/0/_state/global-xxx.st 文件
        return MetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
    }

    /**
     * Writes the index state.
     *
     * This method is public for testing purposes.
     */
    public void writeIndex(String reason, IndexMetaData indexMetaData) throws IOException { // NOTE: htt, 索引元信息，一个节点下所有路径都会写入，即所有路径下同一个索引元信息是一致
        final Index index = indexMetaData.getIndex();
        logger.trace("[{}] writing state, reason [{}]", index, reason);
        try {
            IndexMetaData.FORMAT.write(indexMetaData,
                nodeEnv.indexPaths(indexMetaData.getIndex())); // NOTE:htt, 存储索引settings/mappings 到state文件，${path}/nodes/0/indices/${index}/_state/state-xx.st中的信息
        } catch (Exception ex) {
            logger.warn(() -> new ParameterizedMessage("[{}]: failed to write index state", index), ex);
            throw new IOException("failed to write state for [" + index + "]", ex);
        }
    }

    /**
     * Writes the global state, *without* the indices states.
     */
    void writeGlobalState(String reason, MetaData metaData) throws IOException { // NOTE: htt, 将节点元信息写入到 当前节点下所有路径元信息文件中， ${data.paths}/nodes/0/_state/global-xx.st
        logger.trace("[_global] writing state, reason [{}]",  reason);
        try {
            MetaData.FORMAT.write(metaData, nodeEnv.nodeDataPaths()); // NOTE: htt, 将节点元信息写入到 当前节点下所有路径元信息文件中， ${data.paths}/nodes/0/_state/global-xx.st
        } catch (Exception ex) {
            logger.warn("[_global]: failed to write global state", ex);
            throw new IOException("failed to write global state", ex);
        }
    }
}
