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
package org.apache.rocketmq.client.consumer;

import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultLitePullConsumerImpl;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.ConsumeMessageTraceHookImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
//ok  拉消费者
public class DefaultLitePullConsumer extends ClientConfig implements LitePullConsumer {

    private final InternalLogger log = ClientLogger.getLog();

    private final DefaultLitePullConsumerImpl defaultLitePullConsumerImpl;
    //属性：消费者组、broker挂起最长时间20s，不推荐修改、消费者连接超时30s，不推荐修改、消费者socket超时10s、消费模式，默认集群、
    //消息队列监听器、偏移量存储、队列分配算法、是否自动提交、拉线程数、
    /**
     * Consumers belonging to the same consumer group share a group id. The consumers in a group then divides the topic
     * as fairly amongst themselves as possible by establishing that each queue is only consumed by a single consumer
     * from the group. If all consumers are from the same group, it functions as a traditional message queue. Each
     * message would be consumed by one consumer of the group only. When multiple consumer groups exist, the flow of the
     * data consumption model aligns with the traditional publish-subscribe model. The messages are broadcast to all
     * consumer groups.
     */
    private String consumerGroup;  //消费者组

    /**
     * Long polling mode, the Consumer connection max suspend time, it is not recommended to modify
     */
    private long brokerSuspendMaxTimeMillis = 1000 * 20;   //broker挂起最长时间20s，不推荐修改

    /**
     * Long polling mode, the Consumer connection timeout(must greater than brokerSuspendMaxTimeMillis), it is not
     * recommended to modify
     */
    private long consumerTimeoutMillisWhenSuspend = 1000 * 30;  //消费者连接超时30s，不推荐修改

    /**
     * The socket timeout in milliseconds
     */
    private long consumerPullTimeoutMillis = 1000 * 10;  //消费者socket超时10s

    /**
     * Consumption pattern,default is clustering
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;  //消费模式，默认集群
    /**
     * Message queue listener
     */
    private MessageQueueListener messageQueueListener;  //消息队列监听器
    /**
     * Offset Storage
     */
    private OffsetStore offsetStore;  //偏移量存储对象，本地存储或者远程broker

    /**
     * Queue allocation algorithm
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();  //队列分配算法
    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;

    /**
     * The flag for auto commit offset
     */
    private boolean autoCommit = true;  //是否自动提交

    /**
     * Pull thread number
     */
    private int pullThreadNums = 20; //拉线程数

    /**
     * Minimum commit offset interval time in milliseconds.
     */
    private static final long MIN_AUTOCOMMIT_INTERVAL_MILLIS = 1000;  //最小提交offset间隔1s

    /**
     * Maximum commit offset interval time in milliseconds.
     */
    private long autoCommitIntervalMillis = 5 * 1000;  //最大提交offset间隔5s

    /**
     * Maximum number of messages pulled each time.
     */
    private int pullBatchSize = 10;  //每次拉消息最大数10个

    /**
     * Flow control threshold for consume request, each consumer will cache at most 10000 consume requests by default.
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     */
    private long pullThresholdForAll = 10000;  //每个消费者默认缓存一万请求

    /**
     * Consume max span offset.
     */
    private int consumeMaxSpan = 2000;  //消费最大跨越offset2000

    /**
     * Flow control threshold on queue level, each message queue will cache at most 1000 messages by default, Consider
     * the {@code pullBatchSize}, the instantaneous value may exceed the limit
     */
    private int pullThresholdForQueue = 1000;  //每个消息队列默认缓存1000请求

    /**
     * Limit the cached message size on queue level, each message queue will cache at most 100 MiB messages by default,
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     *
     * <p>
     * The size of a message only measured by message body, so it's not accurate
     */
    private int pullThresholdSizeForQueue = 100;  //每个消息队列默认缓存100M，因为消息只计算消息体，所以这个值不准确

    /**
     * The poll timeout in milliseconds
     */
    private long pollTimeoutMillis = 1000 * 5;  //消息弹栈超时时间5s

    /**
     * Interval time in in milliseconds for checking changes in topic metadata.
     */
    private long topicMetadataCheckIntervalMillis = 30 * 1000;  //主题元数据检查改变间隔30s

    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;  //从哪消费，默认从上一个偏移量

    /**
     * Backtracking consumption time with second precision. Time format is 20131223171201<br> Implying Seventeen twelve
     * and 01 seconds on December 23, 2013 year<br> Default backtracking consumption time Half an hour ago.
     */
    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));

    /**
     * Interface of asynchronous transfer data
     */
    private TraceDispatcher traceDispatcher = null;  //追踪分发器

    /**
     * The flag for message trace
     */
    private boolean enableMsgTrace = false;  //是否开启追踪

    /**
     * The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    private String customizedTraceTopic;   //消息追踪主题

    //构造函数，可以空参、指定组名，指定钩子，组名和钩子、命名空间-组名-钩子。
    /**
     * Default constructor.
     */
    public DefaultLitePullConsumer() {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, null);
    }

    /**
     * Constructor specifying consumer group.
     *
     * @param consumerGroup Consumer group.
     */
    public DefaultLitePullConsumer(final String consumerGroup) {
        this(null, consumerGroup, null);
    }

    /**
     * Constructor specifying RPC hook.
     *
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultLitePullConsumer(RPCHook rpcHook) {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, rpcHook);
    }

    /**
     * Constructor specifying consumer group, RPC hook
     *
     * @param consumerGroup Consumer group.
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultLitePullConsumer(final String consumerGroup, RPCHook rpcHook) {
        this(null, consumerGroup, rpcHook);
    }

    /**
     * Constructor specifying namespace, consumer group and RPC hook.
     *
     * @param consumerGroup Consumer group.
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultLitePullConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook) {
        this.namespace = namespace;
        this.consumerGroup = consumerGroup;
        defaultLitePullConsumerImpl = new DefaultLitePullConsumerImpl(this, rpcHook);
    }

    //ok  开始方法
    @Override
    public void start() throws MQClientException {
        //设置追踪分发器和消费者组名
        setTraceDispatcher();
        setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
        //调用实现类的start
        this.defaultLitePullConsumerImpl.start();
        if (null != traceDispatcher) {
            try {
                //开始追踪
                traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    //ok  关闭方法
    @Override
    public void shutdown() {
        //调用实现类的关闭和追踪分发器的关闭
        this.defaultLitePullConsumerImpl.shutdown();
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
    }

    @Override
    public boolean isRunning() {
        return this.defaultLitePullConsumerImpl.isRunning();
    }

    //ok  订阅
    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultLitePullConsumerImpl.subscribe(withNamespace(topic), subExpression);
    }

    //ok  订阅
    @Override
    public void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {
        this.defaultLitePullConsumerImpl.subscribe(withNamespace(topic), messageSelector);
    }

    //ok  取消订阅
    @Override
    public void unsubscribe(String topic) {
        this.defaultLitePullConsumerImpl.unsubscribe(withNamespace(topic));
    }

    //  ok  分配消息队列
    @Override
    public void assign(Collection<MessageQueue> messageQueues) {
        defaultLitePullConsumerImpl.assign(queuesWithNamespace(messageQueues));
    }

    //ok  弹出消息列表
    @Override
    public List<MessageExt> poll() {
        return defaultLitePullConsumerImpl.poll(this.getPollTimeoutMillis());
    }

    @Override
    public List<MessageExt> poll(long timeout) {
        return defaultLitePullConsumerImpl.poll(timeout);
    }

    //ok  寻找
    @Override
    public void seek(MessageQueue messageQueue, long offset) throws MQClientException {
        this.defaultLitePullConsumerImpl.seek(queueWithNamespace(messageQueue), offset);
    }

    //ok  暂停
    @Override
    public void pause(Collection<MessageQueue> messageQueues) {
        this.defaultLitePullConsumerImpl.pause(queuesWithNamespace(messageQueues));
    }

    //ok  重新开始
    @Override
    public void resume(Collection<MessageQueue> messageQueues) {
        this.defaultLitePullConsumerImpl.resume(queuesWithNamespace(messageQueues));
    }

    //ok  根据主题匹配消息队列
    @Override
    public Collection<MessageQueue> fetchMessageQueues(String topic) throws MQClientException {
        return this.defaultLitePullConsumerImpl.fetchMessageQueues(withNamespace(topic));
    }

    //ok  按时间戳搜寻偏移量
    @Override
    public Long offsetForTimestamp(MessageQueue messageQueue, Long timestamp) throws MQClientException {
        return this.defaultLitePullConsumerImpl.searchOffset(queueWithNamespace(messageQueue), timestamp);
    }

    //ok  注册主题和listener
    @Override
    public void registerTopicMessageQueueChangeListener(String topic,
        TopicMessageQueueChangeListener topicMessageQueueChangeListener) throws MQClientException {
        this.defaultLitePullConsumerImpl.registerTopicMessageQueueChangeListener(withNamespace(topic), topicMessageQueueChangeListener);
    }

    //ok  同步提交
    @Override
    public void commitSync() {
        this.defaultLitePullConsumerImpl.commitAll();
    }

    //ok  提交并返回offset
    @Override
    public Long committed(MessageQueue messageQueue) throws MQClientException {
        return this.defaultLitePullConsumerImpl.committed(queueWithNamespace(messageQueue));
    }

    //ok  更新namesrv地址
    @Override
    public void updateNameServerAddress(String nameServerAddress) {
        this.defaultLitePullConsumerImpl.updateNameServerAddr(nameServerAddress);
    }

    //ok  从头搜索
    @Override
    public void seekToBegin(MessageQueue messageQueue) throws MQClientException {
        this.defaultLitePullConsumerImpl.seekToBegin(queueWithNamespace(messageQueue));
    }

    //ok  从尾搜索
    @Override
    public void seekToEnd(MessageQueue messageQueue) throws MQClientException {
        this.defaultLitePullConsumerImpl.seekToEnd(queueWithNamespace(messageQueue));
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public boolean isConnectBrokerByUser() {
        return this.defaultLitePullConsumerImpl.getPullAPIWrapper().isConnectBrokerByUser();
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.defaultLitePullConsumerImpl.getPullAPIWrapper().setConnectBrokerByUser(connectBrokerByUser);
    }

    public long getDefaultBrokerId() {
        return this.defaultLitePullConsumerImpl.getPullAPIWrapper().getDefaultBrokerId();
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultLitePullConsumerImpl.getPullAPIWrapper().setDefaultBrokerId(defaultBrokerId);
    }

    public int getPullThreadNums() {
        return pullThreadNums;
    }

    public void setPullThreadNums(int pullThreadNums) {
        this.pullThreadNums = pullThreadNums;
    }

    public long getAutoCommitIntervalMillis() {
        return autoCommitIntervalMillis;
    }

    public void setAutoCommitIntervalMillis(long autoCommitIntervalMillis) {
        if (autoCommitIntervalMillis >= MIN_AUTOCOMMIT_INTERVAL_MILLIS) {
            this.autoCommitIntervalMillis = autoCommitIntervalMillis;
        }
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public long getPullThresholdForAll() {
        return pullThresholdForAll;
    }

    public void setPullThresholdForAll(long pullThresholdForAll) {
        this.pullThresholdForAll = pullThresholdForAll;
    }

    public int getConsumeMaxSpan() {
        return consumeMaxSpan;
    }

    public void setConsumeMaxSpan(int consumeMaxSpan) {
        this.consumeMaxSpan = consumeMaxSpan;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }

    public int getPullThresholdSizeForQueue() {
        return pullThresholdSizeForQueue;
    }

    public void setPullThresholdSizeForQueue(int pullThresholdSizeForQueue) {
        this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public long getBrokerSuspendMaxTimeMillis() {
        return brokerSuspendMaxTimeMillis;
    }

    public long getPollTimeoutMillis() {
        return pollTimeoutMillis;
    }

    public void setPollTimeoutMillis(long pollTimeoutMillis) {
        this.pollTimeoutMillis = pollTimeoutMillis;
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public MessageQueueListener getMessageQueueListener() {
        return messageQueueListener;
    }

    public void setMessageQueueListener(MessageQueueListener messageQueueListener) {
        this.messageQueueListener = messageQueueListener;
    }

    public long getConsumerPullTimeoutMillis() {
        return consumerPullTimeoutMillis;
    }

    public void setConsumerPullTimeoutMillis(long consumerPullTimeoutMillis) {
        this.consumerPullTimeoutMillis = consumerPullTimeoutMillis;
    }

    public long getConsumerTimeoutMillisWhenSuspend() {
        return consumerTimeoutMillisWhenSuspend;
    }

    public void setConsumerTimeoutMillisWhenSuspend(long consumerTimeoutMillisWhenSuspend) {
        this.consumerTimeoutMillisWhenSuspend = consumerTimeoutMillisWhenSuspend;
    }

    public long getTopicMetadataCheckIntervalMillis() {
        return topicMetadataCheckIntervalMillis;
    }

    public void setTopicMetadataCheckIntervalMillis(long topicMetadataCheckIntervalMillis) {
        this.topicMetadataCheckIntervalMillis = topicMetadataCheckIntervalMillis;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        if (consumeFromWhere != ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET
            && consumeFromWhere != ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
            && consumeFromWhere != ConsumeFromWhere.CONSUME_FROM_TIMESTAMP) {
            throw new RuntimeException("Invalid ConsumeFromWhere Value", null);
        }
        this.consumeFromWhere = consumeFromWhere;
    }

    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }

    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }

    public TraceDispatcher getTraceDispatcher() {
        return traceDispatcher;
    }

    public void setCustomizedTraceTopic(String customizedTraceTopic) {
        this.customizedTraceTopic = customizedTraceTopic;
    }

    private void setTraceDispatcher() {
        if (isEnableMsgTrace()) {
            try {
                AsyncTraceDispatcher traceDispatcher = new AsyncTraceDispatcher(consumerGroup, TraceDispatcher.Type.CONSUME, customizedTraceTopic, null);
                traceDispatcher.getTraceProducer().setUseTLS(this.isUseTLS());
                this.traceDispatcher = traceDispatcher;
                this.defaultLitePullConsumerImpl.registerConsumeMessageHook(
                    new ConsumeMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    public String getCustomizedTraceTopic() {
        return customizedTraceTopic;
    }

    public boolean isEnableMsgTrace() {
        return enableMsgTrace;
    }

    public void setEnableMsgTrace(boolean enableMsgTrace) {
        this.enableMsgTrace = enableMsgTrace;
    }
}
