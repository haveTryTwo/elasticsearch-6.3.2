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

import org.elasticsearch.Assertions;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.engine.EngineException;

import java.util.concurrent.locks.Lock;

/**
 * Releasable lock used inside of Engine implementations
 */
public class ReleasableLock implements Releasable { // NOTE: htt, releasable lock liking reference lock
    private final Lock lock;


    // a per-thread count indicating how many times the thread has entered the lock; only works if assertions are enabled
    private final ThreadLocal<Integer> holdingThreads; // NOTE: htt, reentry lock, 记录当前线程调用了多少次锁

    public ReleasableLock(Lock lock) {
        this.lock = lock;
        if (Assertions.ENABLED) {
            holdingThreads = new ThreadLocal<>();
        } else {
            holdingThreads = null;
        }
    }

    @Override
    public void close() { // NOTE: htt, try-with-resources block 临时对象作用域退出后会自动调用该函数，当前场景将 主要用在 try-with-resources block 临时对象退出则调用该函数
        lock.unlock();
        assert removeCurrentThread();
    }


    public ReleasableLock acquire() throws EngineException {
        lock.lock();
        assert addCurrentThread(); // NOTE: htt, 次数+1
        return this;
    }

    private boolean addCurrentThread() {
        final Integer current = holdingThreads.get();
        holdingThreads.set(current == null ? 1 : current + 1);
        return true;
    }

    private boolean removeCurrentThread() {
        final Integer count = holdingThreads.get();
        assert count != null && count > 0;
        if (count == 1) {
            holdingThreads.remove(); // NOTE: htt, 如果已经处理完则删除
        } else {
            holdingThreads.set(count - 1); // NOTE: htt, 如果还有重入次数，则减1
        }
        return true;
    }

    public boolean isHeldByCurrentThread() {
        if (holdingThreads == null) {
            throw new UnsupportedOperationException("asserts must be enabled");
        }
        final Integer count = holdingThreads.get();
        return count != null && count > 0;
    }
}
