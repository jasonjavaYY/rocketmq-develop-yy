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
package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
//ok  实现了一个等待唤醒
public class WaitNotifyObject {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected final ConcurrentHashMap<Long/* thread id */, AtomicBoolean/* notified */> waitingThreadTable =
        new ConcurrentHashMap<Long, AtomicBoolean>(16);
    //用于记录是否已经唤醒
    protected AtomicBoolean hasNotified = new AtomicBoolean(false);

    //ok  唤醒方法
    public void wakeup() {
        //CAS设置为已唤醒状态，如果设置成功就调用原生notify
        boolean needNotify = hasNotified.compareAndSet(false, true);
        if (needNotify) {
            synchronized (this) {
                this.notify();
            }
        }
    }

    //等待运行
    protected void waitForRunning(long interval) {
        //首先通过CAS将是否唤醒设置为否，代表处于等待状态，如果CAS设置成功直接返回
        if (this.hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }
        //否则上锁
        synchronized (this) {
            try {
                //再次尝试CAS设置
                if (this.hasNotified.compareAndSet(true, false)) {
                    this.onWaitEnd();
                    return;
                }
                //如果又失败了，就调用原生等待方法，阻塞线程
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                //最终退出时设置为没唤醒状态
                this.hasNotified.set(false);
                this.onWaitEnd();
            }
        }
    }

    protected void onWaitEnd() {
    }

    //ok  唤醒所有
    public void wakeupAll() {
        boolean needNotify = false;
        //遍历等待线程列表，尝试CAS设置为唤醒状态，如果有一个成功，说明存在线程需要被唤醒，就唤醒所有
        for (Map.Entry<Long,AtomicBoolean> entry : this.waitingThreadTable.entrySet()) {
            if (entry.getValue().compareAndSet(false, true)) {
                needNotify = true;
            }
        }
        if (needNotify) {
            synchronized (this) {
                this.notifyAll();
            }
        }
    }

    //ok  全部等待运行，但其实该方法内只调用了某个线程
    public void allWaitForRunning(long interval) {
        //获取当前线程id，根据线程id获取是否唤醒原子对象，如果拿不到就初始化设置成false
        long currentThreadId = Thread.currentThread().getId();
        AtomicBoolean notified = this.waitingThreadTable.computeIfAbsent(currentThreadId, k -> new AtomicBoolean(false));
        //尝试将原子类设置为未唤醒，如果成功，直接返回
        if (notified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }
        synchronized (this) {
            //否则上锁再次尝试将原子类设置为未唤醒，成功直接返回，失败就调用原生wait等待
            try {
                if (notified.compareAndSet(true, false)) {
                    this.onWaitEnd();
                    return;
                }
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                //最后强制将原子类设置为未唤醒
                notified.set(false);
                this.onWaitEnd();
            }
        }
    }

    public void removeFromWaitingThreadTable() {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            this.waitingThreadTable.remove(currentThreadId);
        }
    }
}
