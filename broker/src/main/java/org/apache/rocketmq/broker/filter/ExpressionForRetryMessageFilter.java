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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Support filter to retry topic.
 * <br>It will decode properties first in order to get real topic.
 */
public class ExpressionForRetryMessageFilter extends ExpressionMessageFilter {
    public ExpressionForRetryMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData,
        ConsumerFilterManager consumerFilterManager) {
        super(subscriptionData, consumerFilterData, consumerFilterManager);
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        //如果订阅数据为空或者是类型过滤模式，返回true
        if (subscriptionData == null) {
            return true;
        }
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }
        //判断订阅数据是否为RETRY主题
        boolean isRetryTopic = subscriptionData.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
        //如果不是RETRY主题并且订阅数据是标签表达式类型，返回true
        if (!isRetryTopic && ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            return true;
        }
        ConsumerFilterData realFilterData = this.consumerFilterData;
        Map<String, String> tempProperties = properties;
        boolean decoded = false;
        if (isRetryTopic) {  //如果是retry主题，根据消息buffer解码得到属性，从属性中获取真实主题
            // retry topic, use original filter data.
            // poor performance to support retry filter.
            if (tempProperties == null && msgBuffer != null) {
                decoded = true;
                tempProperties = MessageDecoder.decodeProperties(msgBuffer);
            }
            String realTopic = tempProperties.get(MessageConst.PROPERTY_RETRY_TOPIC);
            //从订阅数据中获取组名，根据主题和组名从过滤管理器中获取过滤数据
            String group = subscriptionData.getTopic().substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
            realFilterData = this.consumerFilterManager.get(realTopic, group);
        }

        // no expression  如果过滤数据为空或者没有表达式，返回true
        if (realFilterData == null || realFilterData.getExpression() == null
            || realFilterData.getCompiledExpression() == null) {
            return true;
        }
        //确保根据消息buffer解码得到属性
        if (!decoded && tempProperties == null && msgBuffer != null) {
            tempProperties = MessageDecoder.decodeProperties(msgBuffer);
        }
        Object ret = null;
        try {
            //根据属性构造消息评估上下文对象，根据过滤数据的表达式判断是否匹配
            MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);
            ret = realFilterData.getCompiledExpression().evaluate(context);
        } catch (Throwable e) {
            log.error("Message Filter error, " + realFilterData + ", " + tempProperties, e);
        }
        log.debug("Pull eval result: {}, {}, {}", ret, realFilterData, tempProperties);
        if (ret == null || !(ret instanceof Boolean)) {
            return false;
        }
        return (Boolean) ret;
    }
}
