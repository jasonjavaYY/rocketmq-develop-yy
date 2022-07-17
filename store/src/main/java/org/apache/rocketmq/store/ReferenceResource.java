/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;
//ok  参考资源
public abstract class ReferenceResource {
    protected final AtomicLong refCount = new AtomicLong(1);
    protected volatile boolean available = true;
    protected volatile boolean cleanupOver = false;
    private volatile long firstShutdownTimestamp = 0;

    //ok  获取资源
    public synchronized boolean hold() {
        //如果可用，就把资源数获取并增加，如果资源数为正返回成功，否则为失败
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                //因为上面增加了1，所以这里要减掉
                this.refCount.getAndDecrement();
            }
        }
        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    //ok  关闭
    public void shutdown(final long intervalForcibly) {
        //如果可用，就置为不可用，更新第一次关闭时间戳，调用释放方法
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            //如果不可用并且资源大于0，就判断当前时间是否超过了第一次关闭时间+关闭间隔，如果超过了，就设置资源数为负，释放
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    //ok  释放
    public void release() {
        //资源数-1并获取，如果还有资源剩余，直接返回
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;
        synchronized (this) {
            //否则上锁，执行清除操作
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
