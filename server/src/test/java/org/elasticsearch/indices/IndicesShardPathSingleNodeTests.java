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

package org.elasticsearch.indices;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeEnvironment.NodePath;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESSingleNodeTestCase;


public class IndicesShardPathSingleNodeTests extends ESSingleNodeTestCase {

    private int dataDirNum = 4;
    private String indexNamePrefix = "index";
    public NodeEnvironment getNodeEnvironment() {
        return getInstanceFromNode(NodeEnvironment.class);
    }

    public void testSelectNewPathForMultiIndexShard() throws Throwable {
        NodeEnvironment env = getNodeEnvironment();
        Map<NodeEnvironment.NodePath, Long> nodeAllPathToLongMap = new HashMap<NodePath, Long>();

        for (int i = 0 ; i < dataDirNum; ++i) {
            String indexName = indexNamePrefix + String.valueOf(i) ;
            assertAcked(client().admin().indices().prepareCreate(indexName)
                    .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
            ensureGreen();

            Index idx = resolveIndex(indexName);
            Path[] idxPath = env.indexPaths(idx);
            logger.info("htt IndexPaths.Length:" + idxPath.length);
            assertEquals(dataDirNum, idxPath.length);
            String indexPathInfo = new String("htt IndexPaths:");
            for (int j = 0; j < idxPath.length; ++j) {
                indexPathInfo += idxPath[j].toString() + ", ";
            }
            logger.info(indexPathInfo);

            Map<NodeEnvironment.NodePath, Long> nodePathToLongMap = env.shardCountPerPath(idx); // NOTE: htt, 获得当前节点下多个目录下索引的shard个
            for (Map.Entry<NodeEnvironment.NodePath, Long> entry : nodePathToLongMap.entrySet()) {
                logger.info("htt Index:{}, shardCountPerPath {}, Count {}", indexName, entry.getKey().path,
                        entry.getValue());
            }

            nodePathToLongMap.forEach(
                    (k, v) -> nodeAllPathToLongMap.merge(k, v, (v1, v2) -> Long.valueOf(v1 + v2))
            );
        }

        for (Map.Entry<NodeEnvironment.NodePath, Long> entry : nodeAllPathToLongMap.entrySet()) {
            logger.info("htt Merge: shardCountPerPath {}, Count {}", entry.getKey().path, entry.getValue());
            assertEquals(1, entry.getValue().intValue());
        }
    }

    @Override
    protected Settings nodeSettings() {
        final Path tempDir = createTempDir();
        String[] dataPaths = new String[dataDirNum];
        for (int i = 0; i < dataPaths.length; ++i) {
            dataPaths[i] = tempDir.resolve("data" + String.valueOf(i)).toString();
        }

        return Settings.builder()
                .putList(Environment.PATH_DATA_SETTING.getKey(), dataPaths).build();
    }
}