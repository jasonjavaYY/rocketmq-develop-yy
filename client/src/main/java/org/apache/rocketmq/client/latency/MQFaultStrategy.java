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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
//ok  MQ故障策略类
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl(); //延迟故障容忍对象

    private boolean sendLatencyFaultEnable = false;   //是否发送延迟故障

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};  //延迟最大值
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};  //不可用持续时间

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    //ok  根据主题和broker名选择一个消息队列
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) {
            //如果开启了发送延迟错误
            try {
                //遍历主题的所有队列，轮询判断，如果这个队列的broker的故障项可用，就返回该队列
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }
                //如果上面没选出来，说明这个主题的所有队列所在的broker都不可用，
                // 就至少选一个broker并获取broker的写队列数
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                //如果写队列不为空，就轮询选择一个队列返回
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    //如果写队列为空，就把这个broker从延迟错误容忍列表删除
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            //如果上面都没选出来，说明这个主题的所有队列所在的broker都不可用，且notBestBroker写队列为空，就轮询返回一个队列
            return tpInfo.selectOneMessageQueue();
        }
        //如果没开启发送延迟错误，就尽量选和传入broker不同的消息队列
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    //ok  更新故障项
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        //如果开启发送延迟故障
        if (this.sendLatencyFaultEnable) {
            //计算不可用持续时间，如果是孤立的就用30s计算，否则用当前延迟计算
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    //ok  计算不可用持续时间
    private long computeNotAvailableDuration(final long currentLatency) {
        //找到当前延迟超过延迟列表中的临界延迟，选择对应的不可用持续时间，找不到，不可用时间就是0
        //比如当前延迟是1100L，则找到的是1000L，对应的不可用时间就是60000L
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
