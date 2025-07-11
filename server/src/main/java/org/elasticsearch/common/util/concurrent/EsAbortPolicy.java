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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.metrics.CounterMetric;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class EsAbortPolicy implements XRejectedExecutionHandler { // NOTE: htt, ESAbort Policy with reject action and increade rejected number
    private final CounterMetric rejected = new CounterMetric();

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (r instanceof AbstractRunnable) {
            if (((AbstractRunnable) r).isForceExecution()) {
                BlockingQueue<Runnable> queue = executor.getQueue();
                if (!(queue instanceof SizeBlockingQueue)) {
                    throw new IllegalStateException("forced execution, but expected a size queue");
                }
                try { 
                    ((SizeBlockingQueue) queue).forcePut(r); // NOTE: htt, 如果是 AbstractRunnable, 并且线程池队列是SizeBlockingQueue，则强制插入到队列， TODO：写请求满足这里的条件，除非异常情况下丢弃请求时对应的请求不是 AbstractRunnable类型； 确认下协调节点刚接受到请求时是否满足这里条件
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("forced execution, but got interrupted", e);
                }
                return;
            }
        }
        rejected.inc();
        throw new EsRejectedExecutionException("rejected execution of " + r + " on " + executor, executor.isShutdown());
    }

    @Override
    public long rejected() {
        return rejected.count();
    }
}
