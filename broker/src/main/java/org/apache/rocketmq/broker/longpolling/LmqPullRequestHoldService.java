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
package org.apache.rocketmq.broker.longpolling;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;


public class LmqPullRequestHoldService extends PullRequestHoldService {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    public LmqPullRequestHoldService(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public String getServiceName() {
        return LmqPullRequestHoldService.class.getSimpleName();
    }

    //ok  检查已有请求
    @Override
    public void checkHoldRequest() {
        for (String key : pullRequestTable.keySet()) {//遍历拉请求列表中的key，如果不含@，直接移除
            int idx = key.lastIndexOf(TOPIC_QUEUEID_SEPARATOR);
            if (idx <= 0 || idx >= key.length() - 1) {
                pullRequestTable.remove(key);
                continue;
            }
            String topic = key.substring(0, idx);  //否则根据key获取主题和队列ID
            int queueId = Integer.parseInt(key.substring(idx + 1));
            //根据主题和队列ID查询队列中最大偏移量，通知消息到达
            final long offset = brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
            try {
                this.notifyMessageArriving(topic, queueId, offset);
            } catch (Throwable e) {
                LOGGER.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
            }
            if (MixAll.isLmq(topic)) {  //如果主题是LMQ，获取该key的拉请求列表，如果列表为空就移除
                ManyPullRequest mpr = pullRequestTable.get(key);
                if (mpr == null || mpr.getPullRequestList() == null || mpr.getPullRequestList().isEmpty()) {
                    pullRequestTable.remove(key);
                }
            }
        }
    }
}
