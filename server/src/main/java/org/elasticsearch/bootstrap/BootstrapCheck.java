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

package org.elasticsearch.bootstrap;

import java.util.Objects;

/**
 * Encapsulates a bootstrap check.
 */
public interface BootstrapCheck { // NOTE: htt, Bootstrap check

    /**
     * Encapsulate the result of a bootstrap check.
     */
    final class BootstrapCheckResult { // NOTE: htt, bootstrap check result which null meanings success

        private final String message;

        private static final BootstrapCheckResult SUCCESS = new BootstrapCheckResult(null);

        public static BootstrapCheckResult success() {
            return SUCCESS;
        }

        public static BootstrapCheckResult failure(final String message) {
            Objects.requireNonNull(message);
            return new BootstrapCheckResult(message);
        }

        private BootstrapCheckResult(final String message) {
            this.message = message;
        }

        public boolean isSuccess() {
            return this == SUCCESS;
        }

        public boolean isFailure() {
            return !isSuccess();
        }

        public String getMessage() {
            assert isFailure();
            assert message != null;
            return message;
        }

    }

    /**
     * Test if the node fails the check.
     *
     * @param context the bootstrap context
     * @return the result of the bootstrap check
     */
    BootstrapCheckResult check(BootstrapContext context);

    default boolean alwaysEnforce() {
        return false;
    }

}
