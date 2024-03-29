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

package org.elasticsearch.cluster.service;

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;

/**
 * PrioritizedRunnable that also has a source string
 */
public abstract class SourcePrioritizedRunnable extends PrioritizedRunnable { // NOTE: htt, prioritizedRunnable with source string
    protected final String source; // NOTE: htt, 记录原始信息

    public SourcePrioritizedRunnable(Priority priority, String source) {
        super(priority);
        this.source = source;
    }

    public String source() {
        return source;
    }

    @Override
    public String toString() {
        return "[" + source + "]";
    }
}
