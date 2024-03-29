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

package org.elasticsearch.transport;

import org.elasticsearch.common.unit.TimeValue;

public class TransportRequestOptions { // NOTE:htt, transport请求option

    private final TimeValue timeout; // NOTE:htt, 超时
    private final boolean compress; // NOTE:htt, 是否压缩
    private final Type type; // NOTE:htt, 请求type

    private TransportRequestOptions(TimeValue timeout, boolean compress, Type type) {
        this.timeout = timeout;
        this.compress = compress;
        this.type = type;
    }

    public TimeValue timeout() {
        return this.timeout;
    }

    public boolean compress() {
        return this.compress;
    }

    public Type type() {
        return this.type;
    }

    public static final TransportRequestOptions EMPTY = new TransportRequestOptions.Builder().build();

    public enum Type { // NOTE:htt, 请求类型
        RECOVERY,
        BULK,
        REG,
        STATE,
        PING
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(TransportRequestOptions options) {
        return new Builder()
                .withTimeout(options.timeout)
                .withCompress(options.compress)
                .withType(options.type());
    }

    public static class Builder { // NOTE:htt, 构建请求
        private TimeValue timeout; // NOTE:htt, 超时时间
        private boolean compress; // NOTE:htt, 是否压缩
        private Type type = Type.REG; // NOTE:htt, 请求类型

        private Builder() {
        }

        public Builder withTimeout(long timeout) {
            return withTimeout(TimeValue.timeValueMillis(timeout));
        }

        public Builder withTimeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withCompress(boolean compress) {
            this.compress = compress;
            return this;
        }

        public Builder withType(Type type) {
            this.type = type;
            return this;
        }

        public TransportRequestOptions build() {
            return new TransportRequestOptions(timeout, compress, type);
        }
    }
}
