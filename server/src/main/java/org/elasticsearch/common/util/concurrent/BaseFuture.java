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

import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transports;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

public abstract class BaseFuture<V> implements Future<V> { // NOTE: htt, base future to get/set and check isDone()/isCancel() using sync

    private static final String BLOCKING_OP_REASON = "Blocking operation";

    /**
     * Synchronization control for AbstractFutures.
     */
    private final Sync<V> sync = new Sync<>();

    /*
    * Improve the documentation of when InterruptedException is thrown. Our
    * behavior matches the JDK's, but the JDK's documentation is misleading.
    */

    /**
     * {@inheritDoc}
     * <p>
     * The default {@link BaseFuture} implementation throws {@code
     * InterruptedException} if the current thread is interrupted before or during
     * the call, even if the value is already available.
     *
     * @throws InterruptedException  if the current thread was interrupted before
     *                               or during the call (optional but recommended).
     * @throws CancellationException {@inheritDoc}
     */
    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            TimeoutException, ExecutionException {
        assert timeout <= 0 ||
            (Transports.assertNotTransportThread(BLOCKING_OP_REASON) &&
                ThreadPool.assertNotScheduleThread(BLOCKING_OP_REASON) &&
                ClusterApplierService.assertNotClusterStateUpdateThread(BLOCKING_OP_REASON) &&
                MasterService.assertNotMasterUpdateThread(BLOCKING_OP_REASON));
        return sync.get(unit.toNanos(timeout));
    }

    /*
    * Improve the documentation of when InterruptedException is thrown. Our
    * behavior matches the JDK's, but the JDK's documentation is misleading.
    */

    /**
     * {@inheritDoc}
     * <p>
     * The default {@link BaseFuture} implementation throws {@code
     * InterruptedException} if the current thread is interrupted before or during
     * the call, even if the value is already available.
     *
     * @throws InterruptedException  if the current thread was interrupted before
     *                               or during the call (optional but recommended).
     * @throws CancellationException {@inheritDoc}
     */
    @Override
    public V get() throws InterruptedException, ExecutionException {
        assert Transports.assertNotTransportThread(BLOCKING_OP_REASON) &&
            ThreadPool.assertNotScheduleThread(BLOCKING_OP_REASON) &&
            ClusterApplierService.assertNotClusterStateUpdateThread(BLOCKING_OP_REASON) &&
            MasterService.assertNotMasterUpdateThread(BLOCKING_OP_REASON);
        return sync.get();
    }

    @Override
    public boolean isDone() {
        return sync.isDone();
    }

    @Override
    public boolean isCancelled() {
        return sync.isCancelled();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (!sync.cancel()) {
            return false;
        }
        done();
        if (mayInterruptIfRunning) {
            interruptTask();
        }
        return true;
    }

    /**
     * Subclasses can override this method to implement interruption of the
     * future's computation. The method is invoked automatically by a successful
     * call to {@link #cancel(boolean) cancel(true)}.
     * <p>
     * The default implementation does nothing.
     *
     * @since 10.0
     */
    protected void interruptTask() {
    }

    /**
     * Subclasses should invoke this method to set the result of the computation
     * to {@code value}.  This will set the state of the future to
     * {@link BaseFuture.Sync#COMPLETED} and call {@link #done()} if the
     * state was successfully changed.
     *
     * @param value the value that was the result of the task.
     * @return true if the state was successfully changed.
     */
    protected boolean set(@Nullable V value) {
        boolean result = sync.set(value);
        if (result) {
            done();
        }
        return result;
    }

    /**
     * Subclasses should invoke this method to set the result of the computation
     * to an error, {@code throwable}.  This will set the state of the future to
     * {@link BaseFuture.Sync#COMPLETED} and call {@link #done()} if the
     * state was successfully changed.
     *
     * @param throwable the exception that the task failed with.
     * @return true if the state was successfully changed.
     * @throws Error if the throwable was an {@link Error}.
     */
    protected boolean setException(Throwable throwable) {
        boolean result = sync.setException(Objects.requireNonNull(throwable));
        if (result) {
            done();
        }

        // If it's an Error, we want to make sure it reaches the top of the
        // call stack, so we rethrow it.

        // we want to notify the listeners we have with errors as well, as it breaks
        // how we work in ES in terms of using assertions
//        if (throwable instanceof Error) {
//            throw (Error) throwable;
//        }
        return result;
    }

    protected void done() {
    }

    /**
     * <p>Following the contract of {@link AbstractQueuedSynchronizer} we create a
     * private subclass to hold the synchronizer.  This synchronizer is used to
     * implement the blocking and waiting calls as well as to handle state changes
     * in a thread-safe manner.  The current state of the future is held in the
     * Sync state, and the lock is released whenever the state changes to either
     * {@link #COMPLETED} or {@link #CANCELLED}.
     * <p>
     * To avoid races between threads doing release and acquire, we transition
     * to the final state in two steps.  One thread will successfully CAS from
     * RUNNING to COMPLETING, that thread will then set the result of the
     * computation, and only then transition to COMPLETED or CANCELLED.
     * <p>
     * We don't use the integer argument passed between acquire methods so we
     * pass around a -1 everywhere.
     */
    static final class Sync<V> extends AbstractQueuedSynchronizer { // NOTE: htt, sync using compareAndSetState to update state and get value/exception
        /* Valid states. */
        static final int RUNNING = 0;  // NOTE: htt, state 默认为0，即RUNNING，所以后续CAS(RUNNING, COMPLETING)更新的时候，则首次会成功，因为RUNNING即0， state默认初始化为0，当前更好是显示初始化下
        static final int COMPLETING = 1;
        static final int COMPLETED = 2;
        static final int CANCELLED = 4;

        private V value;  // htt: htt, 设置结果
        private Throwable exception;

        /*
        * Acquisition succeeds if the future is done, otherwise it fails.
        */
        @Override
        protected int tryAcquireShared(int ignored) { // NOTE: htt, acquire shard if success
            if (isDone()) { // NOTE: htt, value has been set, then 可以获得 acquire shared，即可以获得数据
                return 1;
            }
            return -1;
        }

        /*
        * We always allow a release to go through, this means the state has been
        * successfully changed and the result is available.
        */
        @Override
        protected boolean tryReleaseShared(int finalState) {
            setState(finalState); // NOTE: htt, 更新状态，比如数据已经写入则更新状态为 COMPLETED 或 CANCELED
            return true;
        }

        /**
         * Blocks until the task is complete or the timeout expires.  Throws a
         * {@link TimeoutException} if the timer expires, otherwise behaves like
         * {@link #get()}.
         */
        V get(long nanos) throws TimeoutException, CancellationException,
                ExecutionException, InterruptedException { // NOTE: htt, 带超时时间等待

            // Attempt to acquire the shared lock with a timeout.
            if (!tryAcquireSharedNanos(-1, nanos)) {
                throw new TimeoutException("Timeout waiting for task.");
            }

            return getValue();
        }

        /**
         * Blocks until {@link #complete(Object, Throwable, int)} has been
         * successfully called.  Throws a {@link CancellationException} if the task
         * was cancelled, or a {@link ExecutionException} if the task completed with
         * an error.
         */
        V get() throws CancellationException, ExecutionException,
                InterruptedException { // NOTE: htt, 不带超时时间等待，如果 !isDone() 会一直等待

            // Acquire the shared lock allowing interruption.
            acquireSharedInterruptibly(-1);
            return getValue();
        }

        /**
         * Implementation of the actual value retrieval.  Will return the value
         * on success, an exception on failure, a cancellation on cancellation, or
         * an illegal state if the synchronizer is in an invalid state.
         */
        private V getValue() throws CancellationException, ExecutionException {
            int state = getState();
            switch (state) {
                case COMPLETED:
                    if (exception != null) { // NOTE: htt, 如果异常被设置，那么就直接返回出错
                        throw new ExecutionException(exception); // NOTE: htt, 返回 ExecutionException(exception)
                    } else {
                        return value;
                    }

                case CANCELLED:
                    throw new CancellationException("Task was cancelled.");

                default:
                    throw new IllegalStateException(
                            "Error, synchronizer in invalid state: " + state);
            }
        }

        /**
         * Checks if the state is {@link #COMPLETED} or {@link #CANCELLED}.
         */
        boolean isDone() {
            return (getState() & (COMPLETED | CANCELLED)) != 0;
        }

        /**
         * Checks if the state is {@link #CANCELLED}.
         */
        boolean isCancelled() {
            return getState() == CANCELLED;
        }

        /**
         * Transition to the COMPLETED state and set the value.
         */
        boolean set(@Nullable V v) {
            return complete(v, null, COMPLETED);
        }

        /**
         * Transition to the COMPLETED state and set the exception.
         */
        boolean setException(Throwable t) {
            return complete(null, t, COMPLETED);
        }

        /**
         * Transition to the CANCELLED state.
         */
        boolean cancel() {
            return complete(null, null, CANCELLED);
        }

        /**
         * Implementation of completing a task.  Either {@code v} or {@code t} will
         * be set but not both.  The {@code finalState} is the state to change to
         * from {@link #RUNNING}.  If the state is not in the RUNNING state we
         * return {@code false} after waiting for the state to be set to a valid
         * final state ({@link #COMPLETED} or {@link #CANCELLED}).
         *
         * @param v          the value to set as the result of the computation.
         * @param t          the exception to set as the result of the computation.
         * @param finalState the state to transition to.
         */
        private boolean complete(@Nullable V v, @Nullable Throwable t,
                                 int finalState) {
            boolean doCompletion = compareAndSetState(RUNNING, COMPLETING); // NOTE: htt, cas更新，只有CAS更新成功才允许设置value
            if (doCompletion) {
                // If this thread successfully transitioned to COMPLETING, set the value
                // and exception and then release to the final state.
                this.value = v;
                this.exception = t;
                releaseShared(finalState);
            } else if (getState() == COMPLETING) { // NOTE: htt, cas冲突则说明有其他程序在处理，当前就是等待完成状态（包括COMPLETED, CANCELED)
                // If some other thread is currently completing the future, block until
                // they are done so we can guarantee completion.
                acquireShared(-1);
            }
            return doCompletion;
        }
    }
}
