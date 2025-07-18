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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

/**
 * This async IO processor allows to batch IO operations and have a single writer processing the write operations.
 * This can be used to ensure that threads can continue with other work while the actual IO operation is still processed
 * by a single worker. A worker in this context can be any caller of the {@link #put(Object, Consumer)} method since it will
 * hijack a worker if nobody else is currently processing queued items. If the internal queue has reached it's capacity incoming threads
 * might be blocked until other items are processed
 */
public abstract class AsyncIOProcessor<Item> { // NOTE: htt, 异步IO，以便批量处理
    private final Logger logger;
    private final ArrayBlockingQueue<Tuple<Item, Consumer<Exception>>> queue; // NOTE: htt, 保存待处理的IO
    private final Semaphore promiseSemaphore = new Semaphore(1);

    protected AsyncIOProcessor(Logger logger, int queueSize) {
        this.logger = logger;
        this.queue = new ArrayBlockingQueue<>(queueSize);
    }

    /**
     * Adds the given item to the queue. The listener is notified once the item is processed
     */
    public final void put(Item item, Consumer<Exception> listener) {
        Objects.requireNonNull(item, "item must not be null");
        Objects.requireNonNull(listener, "listener must not be null");
        // the algorithm here tires to reduce the load on each individual caller.
        // we try to have only one caller that processes pending items to disc while others just add to the queue but
        // at the same time never overload the node by pushing too many items into the queue.

        // we first try make a promise that we are responsible for the processing
        final boolean promised = promiseSemaphore.tryAcquire();
        final Tuple<Item, Consumer<Exception>> itemTuple = new Tuple<>(item, listener);
        if (promised == false) {
            // in this case we are not responsible and can just block until there is space
            try {
                queue.put(new Tuple<>(item, listener));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.accept(e);
            }
        }

        // here we have to try to make the promise again otherwise there is a race when a thread puts an entry without making the promise
        // while we are draining that mean we might exit below too early in the while loop if the drainAndSync call is fast.
        if (promised || promiseSemaphore.tryAcquire()) {
            final List<Tuple<Item, Consumer<Exception>>> candidates = new ArrayList<>();
            try {
                if (promised) {
                    // we are responsible for processing we don't need to add the tuple to the queue we can just add it to the candidates
                    candidates.add(itemTuple);
                }
                // since we made the promise to process we gotta do it here at least once
                drainAndProcess(candidates); // NOTE: htt, 转移并处理请求
            } finally {
                promiseSemaphore.release(); // now to ensure we are passing it on we release the promise so another thread can take over
            }
            while (queue.isEmpty() == false && promiseSemaphore.tryAcquire()) {
                // yet if the queue is not empty AND nobody else has yet made the promise to take over we continue processing
                try {
                    drainAndProcess(candidates);
                } finally {
                    promiseSemaphore.release();
                }
            }
        }
    }

    private void drainAndProcess(List<Tuple<Item, Consumer<Exception>>> candidates) {
        queue.drainTo(candidates); // NOTE: htt, 将请求转移到 candidates并进行写入处理
        processList(candidates); // NOTE: htt, 处理请求
        candidates.clear();
    }

    private void processList(List<Tuple<Item, Consumer<Exception>>> candidates) {
        Exception exception = null;
        if (candidates.isEmpty() == false) {
            try {
                write(candidates); // NOTE: htt, 执行后续io处理
            } catch (Exception ex) { // if this fails we are in deep shit - fail the request
                logger.debug("failed to write candidates", ex);
                // this exception is passed to all listeners - we don't retry. if this doesn't work we are in deep shit
                exception = ex;
            }
        }
        for (Tuple<Item, Consumer<Exception>> tuple : candidates) {
            Consumer<Exception> consumer = tuple.v2();
            try {
                consumer.accept(exception);
            } catch (Exception ex) {
                logger.warn("failed to notify callback", ex);
            }
        }
    }

    /**
     * Writes or processes the items out or to disk.
     */
    protected abstract void write(List<Tuple<Item, Consumer<Exception>>> candidates) throws IOException;
}
