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

public interface ConsoleCtrlHandler {  // NOTE: htt, ctrl hanlde

    int CTRL_CLOSE_EVENT = 2;

    /**
     * Handles the Ctrl event.
     *
     * @param code the code corresponding to the Ctrl sent.
     * @return true if the handler processed the event, false otherwise. If false, the next handler will be called.
     */
    boolean handle(int code);
}
