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
package org.elasticsearch.discovery;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.Set;

/**
 * Allows to wait for all nodes to reply to the publish of a new cluster state
 * and notifies the {@link org.elasticsearch.discovery.Discovery.AckListener}
 * so that the cluster state update can be acknowledged
 */
public class AckClusterStatePublishResponseHandler extends BlockingClusterStatePublishResponseHandler { //  htt, 等待publish 列表节点回包，并对每个节点回包执行listener监听处理

    private static final Logger logger = ESLoggerFactory.getLogger(AckClusterStatePublishResponseHandler.class.getName());

    private final Discovery.AckListener ackListener; // NOTE: htt, ack listener to deal with response of all nodes of publish

    /**
     * Creates a new AckClusterStatePublishResponseHandler
     * @param publishingToNodes the set of nodes to which the cluster state will be published and should respond
     * @param ackListener the {@link org.elasticsearch.discovery.Discovery.AckListener} to notify for each response
     *                    gotten from non master nodes
     */
    public AckClusterStatePublishResponseHandler(Set<DiscoveryNode> publishingToNodes, Discovery.AckListener ackListener) {
        //Don't count the master as acknowledged, because it's not done yet
        //otherwise we might end up with all the nodes but the master holding the latest cluster state
        super(publishingToNodes);
        this.ackListener = ackListener;
    }

    @Override
    public void onResponse(DiscoveryNode node) {
        super.onResponse(node);
        onNodeAck(ackListener, node, null);
    }

    @Override
    public void onFailure(DiscoveryNode node, Exception e) {
        try {
            super.onFailure(node, e);
        } finally {
            onNodeAck(ackListener, node, e);
        }
    }

    private void onNodeAck(final Discovery.AckListener ackListener, DiscoveryNode node, Exception e) {
        try {
            ackListener.onNodeAck(node, e); // NOTE:htt, 节点Ack后的处理
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.debug(() -> new ParameterizedMessage("error while processing ack for node [{}]", node), inner);
        }
    }
}
