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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.CommitLog.MessageExtEncoder;

/**
 * not-thread-safe
 */
//ok  多分发
public class MultiDispatch {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final StringBuilder keyBuilder = new StringBuilder();
    private final DefaultMessageStore messageStore;
    private final CommitLog commitLog;

    public MultiDispatch(DefaultMessageStore messageStore, CommitLog commitLog) {
        this.messageStore = messageStore;
        this.commitLog = commitLog;
    }

    //ok  根据队列名和消息Inner的属性拼串构造队列key
    public String queueKey(String queueName, MessageExtBrokerInner msgInner) {
        keyBuilder.setLength(0);
        keyBuilder.append(queueName);
        keyBuilder.append('-');
        int queueId = msgInner.getQueueId();
        if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
            queueId = 0;
        }
        keyBuilder.append(queueId);
        return keyBuilder.toString();
    }

    //封装多分发
    public boolean wrapMultiDispatch(final MessageExtBrokerInner msgInner) {
        //如果消息存储配置没开启多分发，直接返回
        if (!messageStore.getMessageStoreConfig().isEnableMultiDispatch()) {
            return true;
        }
        //否则根据消息inner获取多分发队列名数组
        String multiDispatchQueue = msgInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return true;
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        Long[] queueOffsets = new Long[queues.length];   //创建队列偏移量数组
        for (int i = 0; i < queues.length; i++) {   //循环对所有队列创建队列key
            String key = queueKey(queues[i], msgInner);
            Long queueOffset;
            try {
                queueOffset = getTopicQueueOffset(key);  //根据队列key获取主题队列偏移量
            } catch (Exception e) {
                return false;
            }
            if (null == queueOffset) {
                //如果偏移量为空，就初始化为0
                queueOffset = 0L;
                //如果开启了lmq并且key属于lmq，就把key和偏移量放入lmq主题队列表中
                if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                    commitLog.getLmqTopicQueueTable().put(key, queueOffset);
                } else {
                    commitLog.getTopicQueueTable().put(key, queueOffset);  //否则放入主题队列表
                }
            }
            queueOffsets[i] = queueOffset;  //记录队列的偏移量
        }
        //给消息设置多队列偏移量
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET,
            StringUtils.join(queueOffsets, MixAll.MULTI_DISPATCH_QUEUE_SPLITTER));
        removeWaitStorePropertyString(msgInner);  //删除等待存储属性
        return rebuildMsgInner(msgInner);  //将消息序列化，重新构建一个消息inner
    }

    //ok  删除等待存储属性
    private void removeWaitStorePropertyString(MessageExtBrokerInner msgInner) {
        //如果消息属性包含WAIT这个key，就移除这个属性
        if (msgInner.getProperties().containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            // There is no need to store "WAIT=true", remove it from propertiesString to save 9 bytes for each message.
            // It works for most case. In some cases msgInner.setPropertiesString invoked later and replace it.
            String waitStoreMsgOKValue = msgInner.getProperties().remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            // Reput to properties, since msgInner.isWaitStoreMsgOK() will be invoked later
            msgInner.getProperties().put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        } else {
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        }
    }

    //重新构建消息inner
    private boolean rebuildMsgInner(MessageExtBrokerInner msgInner) {
        //获取消息的编码器，将消息编码，返回编码结果，如果成功了会返回null
        MessageExtEncoder encoder = this.commitLog.getPutMessageThreadLocal().get().getEncoder();
        PutMessageResult encodeResult = encoder.encode(msgInner);
        if (encodeResult != null) {
            LOGGER.error("rebuild msgInner for multiDispatch", encodeResult);
            return false;
        }
        //给消息设置编码buf
        msgInner.setEncodedBuff(encoder.getEncoderBuffer());
        return true;

    }

    //ok  更新多队列偏移量
    public void updateMultiQueueOffset(final MessageExtBrokerInner msgInner) {
        //如果没开启多分发，直接返回
        if (!messageStore.getMessageStoreConfig().isEnableMultiDispatch()) {
            return;
        }
        //获取消息的多分发队列名
        String multiDispatchQueue = msgInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        //获取消息的多队列偏移量
        String multiQueueOffset = msgInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiQueueOffset)) {
            LOGGER.error("[bug] no multiQueueOffset when updating {}", msgInner.getTopic());
            return;
        }
        //获取队列名数组和队列偏移量数组，二者一一对应
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        if (queues.length != queueOffsets.length) {
            LOGGER.error("[bug] num is not equal when updateMultiQueueOffset {}", msgInner.getTopic());
            return;
        }
        for (int i = 0; i < queues.length; i++) {  //遍历队列
            String key = queueKey(queues[i], msgInner);  //创建队列key
            long queueOffset = Long.parseLong(queueOffsets[i]);  //获取队列偏移量
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                //如果开启了lmq并且队列是lmq，就更新lmq的主题队列表
                commitLog.getLmqTopicQueueTable().put(key, ++queueOffset);
            } else {   //否则更新主题队列表
                commitLog.getTopicQueueTable().put(key, ++queueOffset);
            }
        }
    }

    private Long getTopicQueueOffset(String key) throws Exception {
        Long offset = null;
        if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
            Long queueNextOffset = commitLog.getLmqTopicQueueTable().get(key);
            if (queueNextOffset != null) {
                offset = queueNextOffset;
            }
        } else {
            offset = commitLog.getTopicQueueTable().get(key);
        }
        return offset;
    }

}
