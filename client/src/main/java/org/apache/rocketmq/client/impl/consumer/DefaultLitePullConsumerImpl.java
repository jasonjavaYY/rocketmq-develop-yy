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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
//ok  拉消费者实现类  日志、消费开始时间戳、过滤消息钩子列表、消费者服务状态、客户端实例、拉API封装、偏移量存储、负载均衡拉实现
//                  订阅类型、异常拉延迟、流控拉延迟、挂起服务拉延迟、任务列表、分配消息队列、主题消息队列改变监听表
//                  主题消息队列映射表、消费请求流控时间、队列流控时间、队列最大跳跃流控时间、下一次自动提交时间
//                  消息队列锁、消费消息钩子列表、
public class DefaultLitePullConsumerImpl implements MQConsumerInner {

    private final InternalLogger log = ClientLogger.getLog();

    private final long consumerStartTimestamp = System.currentTimeMillis();

    private final RPCHook rpcHook;

    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    protected MQClientInstance mQClientFactory;

    private PullAPIWrapper pullAPIWrapper;

    private OffsetStore offsetStore;

    private RebalanceImpl rebalanceImpl = new RebalanceLitePullImpl(this);

    private enum SubscriptionType {
        NONE, SUBSCRIBE, ASSIGN
    }

    private static final String NOT_RUNNING_EXCEPTION_MESSAGE = "The consumer not running, please start it first.";

    private static final String SUBSCRIPTION_CONFLICT_EXCEPTION_MESSAGE = "Subscribe and assign are mutually exclusive.";
    /**
     * the type of subscription
     */
    private SubscriptionType subscriptionType = SubscriptionType.NONE;
    /**
     * Delay some time when exception occur
     */
    private long pullTimeDelayMillsWhenException = 1000;
    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_PAUSE = 1000;

    private static final long PULL_TIME_DELAY_MILLS_ON_EXCEPTION = 3 * 1000;

    private DefaultLitePullConsumer defaultLitePullConsumer;

    private final ConcurrentMap<MessageQueue, PullTaskImpl> taskTable =
        new ConcurrentHashMap<MessageQueue, PullTaskImpl>();

    private AssignedMessageQueue assignedMessageQueue = new AssignedMessageQueue();

    private final BlockingQueue<ConsumeRequest> consumeRequestCache = new LinkedBlockingQueue<ConsumeRequest>();

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private final ScheduledExecutorService scheduledExecutorService;

    private Map<String, TopicMessageQueueChangeListener> topicMessageQueueChangeListenerMap = new HashMap<String, TopicMessageQueueChangeListener>();

    private Map<String, Set<MessageQueue>> messageQueuesForTopic = new HashMap<String, Set<MessageQueue>>();

    private long consumeRequestFlowControlTimes = 0L;

    private long queueFlowControlTimes = 0L;

    private long queueMaxSpanFlowControlTimes = 0L;

    private long nextAutoCommitDeadline = -1L;

    private final MessageQueueLock messageQueueLock = new MessageQueueLock();

    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();

    //ok  构造方法  创建2个周期线程池
    public DefaultLitePullConsumerImpl(final DefaultLitePullConsumer defaultLitePullConsumer, final RPCHook rpcHook) {
        this.defaultLitePullConsumer = defaultLitePullConsumer;
        this.rpcHook = rpcHook;
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
            this.defaultLitePullConsumer.getPullThreadNums(),
            new ThreadFactoryImpl("PullMsgThread-" + this.defaultLitePullConsumer.getConsumerGroup())
        );
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "MonitorMessageQueueChangeThread");
            }
        });
        this.pullTimeDelayMillsWhenException = defaultLitePullConsumer.getPullTimeDelayMillsWhenException();
    }

    //ok  注册消费消息钩子
    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        //把钩子加入钩子列表中
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    //ok  执行钩子before操作  调用消费消息追踪钩子的before
    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                    log.error("consumeMessageHook {} executeHookBefore exception", hook.hookName(), e);
                }
            }
        }
    }

    //ok  执行钩子after操作  调用消费消息追踪钩子的after
    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    log.error("consumeMessageHook {} executeHookAfter exception", hook.hookName(), e);
                }
            }
        }
    }

    private void checkServiceState() {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new IllegalStateException(NOT_RUNNING_EXCEPTION_MESSAGE);
        }
    }

    public void updateNameServerAddr(String newAddresses) {
        this.mQClientFactory.getMQClientAPIImpl().updateNameServerAddressList(newAddresses);
    }

    private synchronized void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE) {
            this.subscriptionType = type;
        } else if (this.subscriptionType != type) {
            throw new IllegalStateException(SUBSCRIPTION_CONFLICT_EXCEPTION_MESSAGE);
        }
    }

    private void updateAssignedMessageQueue(String topic, Set<MessageQueue> assignedMessageQueue) {
        this.assignedMessageQueue.updateAssignedMessageQueue(topic, assignedMessageQueue);
    }

    //ok  更新某个主题的拉取任务
    private void updatePullTask(String topic, Set<MessageQueue> mqNewSet) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            //遍历任务列表，找到匹配主题的任务，如果新MQ集合不包含该任务对应的MQ，就删除任务
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                if (!mqNewSet.contains(next.getKey())) {
                    next.getValue().setCancelled(true);
                    it.remove();
                }
            }
        }
        //用新MQ集合开启拉取任务
        startPullTask(mqNewSet);
    }

    class MessageQueueListenerImpl implements MessageQueueListener {
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            MessageModel messageModel = defaultLitePullConsumer.getMessageModel();
            switch (messageModel) {
                case BROADCASTING:
                    updateAssignedMessageQueue(topic, mqAll);
                    updatePullTask(topic, mqAll);
                    break;
                case CLUSTERING:
                    updateAssignedMessageQueue(topic, mqDivided);
                    updatePullTask(topic, mqDivided);
                    break;
                default:
                    break;
            }
        }
    }

    //ok  关闭
    public synchronized void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                //持久化消费者偏移量
                persistConsumerOffset();
                //取消注册client，关闭两个线程池
                this.mQClientFactory.unregisterConsumer(this.defaultLitePullConsumer.getConsumerGroup());
                scheduledThreadPoolExecutor.shutdown();
                scheduledExecutorService.shutdown();
                //关闭client
                this.mQClientFactory.shutdown();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                log.info("the consumer [{}] shutdown OK", this.defaultLitePullConsumer.getConsumerGroup());
                break;
            default:
                break;
        }
    }

    public synchronized boolean isRunning() {
        return this.serviceState == ServiceState.RUNNING;
    }

    //ok  开始
    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            //如果是刚创建状态
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;
                //检查配置
                this.checkConfig();
                //如果消费者是集群模式，就把消费者实例id改为pid
                if (this.defaultLitePullConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultLitePullConsumer.changeInstanceNameToPID();
                }
                //初始化client、rebalance、拉取API、偏移量存储、开启client、开始定时任务
                initMQClientFactory();
                initRebalanceImpl();
                initPullAPIWrapper();
                initOffsetStore();
                mQClientFactory.start();
                startScheduleTask();
                //消费者服务状态改为running
                this.serviceState = ServiceState.RUNNING;
                log.info("the consumer [{}] start OK", this.defaultLitePullConsumer.getConsumerGroup());
                //执行running之后的操作：如果订阅状态是订阅，就更新主题订阅信息，
                // 如果订阅类型是assign，就更新assign拉取任务遍历主题MQ改变监听map，
                // 获取主题的MQ集合，放入MQ主题map中，检查监听器状态
                operateAfterRunning();
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PullConsumer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }
    }

    //ok  初始化client
    private void initMQClientFactory() throws MQClientException {
        //获取client，注册消费者到消费者组
        this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultLitePullConsumer, this.rpcHook);
        boolean registerOK = mQClientFactory.registerConsumer(this.defaultLitePullConsumer.getConsumerGroup(), this);
        if (!registerOK) {
            this.serviceState = ServiceState.CREATE_JUST;

            throw new MQClientException("The consumer group[" + this.defaultLitePullConsumer.getConsumerGroup()
                + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                null);
        }
    }
    //ok  初始化rebalance
    private void initRebalanceImpl() {
        //给rebalance设置组名、消息模式、分配MQ策略、client
        this.rebalanceImpl.setConsumerGroup(this.defaultLitePullConsumer.getConsumerGroup());
        this.rebalanceImpl.setMessageModel(this.defaultLitePullConsumer.getMessageModel());
        this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultLitePullConsumer.getAllocateMessageQueueStrategy());
        this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);
    }

    //ok  初始化拉取API
    private void initPullAPIWrapper() {
        //创建一个拉API，注册过滤消息钩子
        this.pullAPIWrapper = new PullAPIWrapper(
            mQClientFactory,
            this.defaultLitePullConsumer.getConsumerGroup(), isUnitMode());
        this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);
    }

    //ok 初始化偏移量存储
    private void initOffsetStore() throws MQClientException {
        //创建offset存储
        if (this.defaultLitePullConsumer.getOffsetStore() != null) {
            this.offsetStore = this.defaultLitePullConsumer.getOffsetStore();
        } else {
            switch (this.defaultLitePullConsumer.getMessageModel()) {
                //消息模式如果是广播，就创建本地offset存储，如果是集群，就创建远程broker存储
                case BROADCASTING:
                    this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                    break;
                case CLUSTERING:
                    this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                    break;
                default:
                    break;
            }
            this.defaultLitePullConsumer.setOffsetStore(this.offsetStore);
        }
        //加载存储
        this.offsetStore.load();
    }

    //ok  开启定时任务
    private void startScheduleTask() {
        scheduledExecutorService.scheduleAtFixedRate(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        //定期获取主题MQ并比较
                        fetchTopicMessageQueuesAndCompare();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchMessageQueuesAndCompare exception", e);
                    }
                }
            }, 1000 * 10, this.getDefaultLitePullConsumer().getTopicMetadataCheckIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    private void operateAfterRunning() throws MQClientException {
        // If subscribe function invoke before start function, then update topic subscribe info after initialization.
        //如果状态是订阅，就更新主题订阅信息
        if (subscriptionType == SubscriptionType.SUBSCRIBE) {
            updateTopicSubscribeInfoWhenSubscriptionChanged();
        }
        // If assign function invoke before start function, then update pull task after initialization.
        //如果订阅类型是assign，就更新assign拉取任务
        if (subscriptionType == SubscriptionType.ASSIGN) {
            updateAssignPullTask(assignedMessageQueue.messageQueues());
        }
        //遍历主题MQ改变监听map，获取主题的MQ集合，放入MQ主题map中
        for (String topic : topicMessageQueueChangeListenerMap.keySet()) {
            Set<MessageQueue> messageQueues = fetchMessageQueues(topic);
            messageQueuesForTopic.put(topic, messageQueues);
        }
        //检查监听器状态
        this.mQClientFactory.checkClientInBroker();
    }

    //ok  检查配置
    private void checkConfig() throws MQClientException {
        // Check consumerGroup
        //检查消费者组是否合法
        Validators.checkGroup(this.defaultLitePullConsumer.getConsumerGroup());

        // Check consumerGroup name is not equal default consumer group name.
        if (this.defaultLitePullConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                "consumerGroup can not equal "
                    + MixAll.DEFAULT_CONSUMER_GROUP
                    + ", please specify another one."
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // Check messageModel is not null.
        if (null == this.defaultLitePullConsumer.getMessageModel()) {
            throw new MQClientException(
                "messageModel is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // Check allocateMessageQueueStrategy is not null
        if (null == this.defaultLitePullConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                "allocateMessageQueueStrategy is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() < this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis()) {
            throw new MQClientException(
                "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
    }

    public PullAPIWrapper getPullAPIWrapper() {
        return pullAPIWrapper;
    }

    //ok 开启拉取任务
    private void startPullTask(Collection<MessageQueue> mqSet) {
        //找到传入的MQ集合中在当前任务列表中不存在的MQ，为其构造拉任务，和MQ匹配放入列表，开启该任务
        for (MessageQueue messageQueue : mqSet) {
            if (!this.taskTable.containsKey(messageQueue)) {
                PullTaskImpl pullTask = new PullTaskImpl(messageQueue);
                this.taskTable.put(messageQueue, pullTask);
                this.scheduledThreadPoolExecutor.schedule(pullTask, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    //ok  更新assign拉任务
    private void updateAssignPullTask(Collection<MessageQueue> mqNewSet) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (!mqNewSet.contains(next.getKey())) {
                next.getValue().setCancelled(true);
                it.remove();
            }
        }

        startPullTask(mqNewSet);
    }

    //ok  当订阅改变时更新主题订阅信息
    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, SubscriptionData> subTable = rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    //ok  订阅主题方法
    public synchronized void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            if (topic == null || "".equals(topic)) {
                throw new IllegalArgumentException("Topic can not be null or empty.");
            }
            //将订阅类型设置为subscribe
            setSubscriptionType(SubscriptionType.SUBSCRIBE);
            //构建订阅数据
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
            //均衡器将主题和订阅数据放入订阅器map
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            //拉消费者设置MQ监听器
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
            //设置均衡器
            assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
            if (serviceState == ServiceState.RUNNING) {
                //如果服务状态是运行，就给所有broker发心跳
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                //执行更新主题订阅信息，具体实现在client/impl/factory中
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }
    }

    //ok  带选择器的订阅方法
    public synchronized void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {
        try {
            if (topic == null || "".equals(topic)) {
                throw new IllegalArgumentException("Topic can not be null or empty.");
            }
            setSubscriptionType(SubscriptionType.SUBSCRIBE);
            //如果传入的选择器为空，就调用不带选择器方法，订阅所有主题
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }
            //否则根据选择器构建订阅数据，其它和不带选择器的订阅一样
            SubscriptionData subscriptionData = FilterAPI.build(topic,
                messageSelector.getExpression(), messageSelector.getExpressionType());
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
            assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
            if (serviceState == ServiceState.RUNNING) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }
    }

    //ok  取消订阅
    public synchronized void unsubscribe(final String topic) {
        //均衡器内的订阅信息map删除该主题，删除该主题的拉任务回调，删除该主题的assignMQ。
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
        removePullTaskCallback(topic);
        assignedMessageQueue.removeAssignedMessageQueue(topic);
    }

    //ok  分配
    public synchronized void assign(Collection<MessageQueue> messageQueues) {
        if (messageQueues == null || messageQueues.isEmpty()) {
            throw new IllegalArgumentException("Message queues can not be null or empty.");
        }
        //设置订阅配型为分配
        setSubscriptionType(SubscriptionType.ASSIGN);
        //更新分配MQ队列
        assignedMessageQueue.updateAssignedMessageQueue(messageQueues);
        if (serviceState == ServiceState.RUNNING) {
            updateAssignPullTask(messageQueues);
        }
    }

    //ok  或许自动提交
    private void maybeAutoCommit() {
        long now = System.currentTimeMillis();
        //如果当前时间戳超过下次自动提交截止时间，就提交所有消息，并更新下次提交截止时间
        if (now >= nextAutoCommitDeadline) {
            commitAll();
            nextAutoCommitDeadline = now + defaultLitePullConsumer.getAutoCommitIntervalMillis();
        }
    }

    //弹出消息列表
    public synchronized List<MessageExt> poll(long timeout) {
        try {
            //检查服务状态是否为running和是否超时
            checkServiceState();
            if (timeout < 0) {
                throw new IllegalArgumentException("Timeout must not be negative");
            }
            //如果拉消费者开启自动提交，就自动提交
            if (defaultLitePullConsumer.isAutoCommit()) {
                maybeAutoCommit();
            }
            long endTime = System.currentTimeMillis() + timeout;
            //从消费请求缓存队列中弹出一个消费请求
            ConsumeRequest consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            //在超时之前不停地弹出消费请求
            if (endTime - System.currentTimeMillis() > 0) {
                while (consumeRequest != null && consumeRequest.getProcessQueue().isDropped()) {
                    consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                    if (endTime - System.currentTimeMillis() <= 0) {
                        break;
                    }
                }
            }
            //最后弹出的一个消费请求如果不为空且没被弃置
            if (consumeRequest != null && !consumeRequest.getProcessQueue().isDropped()) {
                //获取该消费请求的消息列表，通过该消费请求的PQ清除消息列表得到偏移量
                List<MessageExt> messages = consumeRequest.getMessageExts();
                long offset = consumeRequest.getProcessQueue().removeMessage(messages);
                //更新消费偏移量
                assignedMessageQueue.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
                //If namespace not null , reset Topic without namespace.
                //如果命名空间不为空，重设消息主题为不带命名空间
                this.resetTopic(messages);
                //返回消息
                return messages;
            }
        } catch (InterruptedException ignore) {

        }

        return Collections.emptyList();
    }

    public void pause(Collection<MessageQueue> messageQueues) {
        assignedMessageQueue.pause(messageQueues);
    }

    public void resume(Collection<MessageQueue> messageQueues) {
        assignedMessageQueue.resume(messageQueues);
    }

    //从MQ的指定偏移量开始拉消息
    public synchronized void seek(MessageQueue messageQueue, long offset) throws MQClientException {
        //如果内存的MQ队列不包含传入的MQ，抛异常
        if (!assignedMessageQueue.messageQueues().contains(messageQueue)) {
            if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                throw new MQClientException("The message queue is not in assigned list, may be rebalancing, message queue: " + messageQueue, null);
            } else {
                throw new MQClientException("The message queue is not in assigned list, message queue: " + messageQueue, null);
            }
        }
        //计算MQ的最大最小偏移量，看传入偏移量是否合理
        long minOffset = minOffset(messageQueue);
        long maxOffset = maxOffset(messageQueue);
        if (offset < minOffset || offset > maxOffset) {
            throw new MQClientException("Seek offset illegal, seek offset = " + offset + ", min offset = " + minOffset + ", max offset = " + maxOffset, null);
        }
        //获取mq的锁，保证某时刻只能有一个消费者消费某个MQ
        final Object objLock = messageQueueLock.fetchLockObject(messageQueue);
        synchronized (objLock) {
            //上锁，清除MQ缓存
            clearMessageQueueInCache(messageQueue);
            //获取该mq的旧拉任务，如果任务不为空，尝试中断任务并将该mq和任务从任务列表清除
            PullTaskImpl oldPullTaskImpl = this.taskTable.get(messageQueue);
            if (oldPullTaskImpl != null) {
                oldPullTaskImpl.tryInterrupt();
                this.taskTable.remove(messageQueue);
            }
            //给mq设置寻找偏移量
            assignedMessageQueue.setSeekOffset(messageQueue, offset);
            if (!this.taskTable.containsKey(messageQueue)) {
                //再次确保任务列表中不包含该mq的任务，因为上面的中断可能失败
                //针对mq构建一个新的拉取任务，把任务和mq放入任务匹配表并执行拉任务
                PullTaskImpl pullTask = new PullTaskImpl(messageQueue);
                this.taskTable.put(messageQueue, pullTask);
                this.scheduledThreadPoolExecutor.schedule(pullTask, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void seekToBegin(MessageQueue messageQueue) throws MQClientException {
        long begin = minOffset(messageQueue);
        this.seek(messageQueue, begin);
    }

    public void seekToEnd(MessageQueue messageQueue) throws MQClientException {
        long end = maxOffset(messageQueue);
        this.seek(messageQueue, end);
    }

    private long maxOffset(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(messageQueue);
    }

    private long minOffset(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().minOffset(messageQueue);
    }

    private void removePullTaskCallback(final String topic) {
        removePullTask(topic);
    }

    private void removePullTask(final String topic) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                next.getValue().setCancelled(true);
                it.remove();
            }
        }
    }

    //ok  提交所有
    public synchronized void commitAll() {
        try {
            //遍历分配MQ的消息列表
            for (MessageQueue messageQueue : assignedMessageQueue.messageQueues()) {
                //获取每个MQ的偏移量
                long consumerOffset = assignedMessageQueue.getConsumerOffset(messageQueue);
                if (consumerOffset != -1) {
                    //根据MQ获取PQ，如果PQ不是废弃状态，就更新MQ的偏移量
                    ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
                    if (processQueue != null && !processQueue.isDropped()) {
                        updateConsumeOffset(messageQueue, consumerOffset);
                    }
                }
            }
            //如果拉消费者消费模式是广播，就持久化所有MQ的偏移量
            if (defaultLitePullConsumer.getMessageModel() == MessageModel.BROADCASTING) {
                offsetStore.persistAll(assignedMessageQueue.messageQueues());
            }
        } catch (Exception e) {
            log.error("An error occurred when update consume offset Automatically.");
        }
    }

    //Ok  更新拉偏移量，前提是mq的seek偏移量是-1
    private void updatePullOffset(MessageQueue messageQueue, long nextPullOffset, ProcessQueue processQueue) {
        if (assignedMessageQueue.getSeekOffset(messageQueue) == -1) {
            assignedMessageQueue.updatePullOffset(messageQueue, nextPullOffset, processQueue);
        }
    }

    private void submitConsumeRequest(ConsumeRequest consumeRequest) {
        try {
            consumeRequestCache.put(consumeRequest);
        } catch (InterruptedException e) {
            log.error("Submit consumeRequest error", e);
        }
    }

    //ok  获取某个MQ的消费偏移量
    private long fetchConsumeOffset(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        long offset = this.rebalanceImpl.computePullFromWhereWithException(messageQueue);
        return offset;
    }

    //ok 已提交 读取mq的偏移量并返回
    public long committed(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        long offset = this.offsetStore.readOffset(messageQueue, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
        if (offset == -2) {
            throw new MQClientException("Fetch consume offset from broker exception", null);
        }
        return offset;
    }

    //ok  清除缓存中的MQ
    private void clearMessageQueueInCache(MessageQueue messageQueue) {
        ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
        if (processQueue != null) {
            processQueue.clear();
        }
        Iterator<ConsumeRequest> iter = consumeRequestCache.iterator();
        while (iter.hasNext()) {
            if (iter.next().getMessageQueue().equals(messageQueue)) {
                iter.remove();
            }
        }
    }
    //ok  下一次拉取偏移量
    private long nextPullOffset(MessageQueue messageQueue) throws MQClientException {
        //先获取seekOffset
        long offset = -1;
        long seekOffset = assignedMessageQueue.getSeekOffset(messageQueue);
        if (seekOffset != -1) {
            //如果seekOffset获取不是-1，就返回这个offset
            offset = seekOffset;
            assignedMessageQueue.updateConsumeOffset(messageQueue, offset);
            assignedMessageQueue.setSeekOffset(messageQueue, -1);
        } else {
            //否则获取pull偏移
            offset = assignedMessageQueue.getPullOffset(messageQueue);
            if (offset == -1) {
                //如果pull偏移是-1，就通过rebalance获取mq的偏移
                offset = fetchConsumeOffset(messageQueue);
            }
        }
        return offset;
    }

    //根据时间戳获取某个mq的偏移量
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    //ok  内部类 拉任务实现类
    public class PullTaskImpl implements Runnable {
        private final MessageQueue messageQueue;
        private volatile boolean cancelled = false;
        private Thread currentThread;

        public PullTaskImpl(final MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        //尝试中断任务
        public void tryInterrupt() {
            setCancelled(true);
            if (currentThread == null) {
                return;
            }
            if (!currentThread.isInterrupted()) {
                currentThread.interrupt();
            }
        }

        @Override
        public void run() {
            //如果任务没被删除
            if (!this.isCancelled()) {
                //获取当前线程
                this.currentThread = Thread.currentThread();
                //是否触发中断延迟
                if (assignedMessageQueue.isPaused(messageQueue)) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_PAUSE, TimeUnit.MILLISECONDS);
                    log.debug("Message Queue: {} has been paused!", messageQueue);
                    return;
                }
                //根据MQ获取PQ
                ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
                //PQ如果为空或者放弃，记录日志返回
                if (null == processQueue || processQueue.isDropped()) {
                    log.info("The message queue not be able to poll, because it's dropped. group={}, messageQueue={}", defaultLitePullConsumer.getConsumerGroup(), this.messageQueue);
                    return;
                }
                //如果消费请求缓存*拉消费者批次>拉取阈值，触发流控
                if ((long) consumeRequestCache.size() * defaultLitePullConsumer.getPullBatchSize() > defaultLitePullConsumer.getPullThresholdForAll()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((consumeRequestFlowControlTimes++ % 1000) == 0) {
                        log.warn("The consume request count exceeds threshold {}, so do flow control, consume request count={}, flowControlTimes={}", consumeRequestCache.size(), consumeRequestFlowControlTimes);
                    }
                    return;
                }
                //计算缓存的消息数和消息大小MB
                long cachedMessageCount = processQueue.getMsgCount().get();
                long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);
                //如果消息数超过了拉消费者阈值，触发流控
                if (cachedMessageCount > defaultLitePullConsumer.getPullThresholdForQueue()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                            defaultLitePullConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                    }
                    return;
                }
                //如果消息大小超过阈值，触发流控
                if (cachedMessageSizeInMiB > defaultLitePullConsumer.getPullThresholdSizeForQueue()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                            defaultLitePullConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                    }
                    return;
                }
                //如果PQ的最大偏移量差距超过了消费者阈值，触发流控
                if (processQueue.getMaxSpan() > defaultLitePullConsumer.getConsumeMaxSpan()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, flowControlTimes={}",
                            processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(), queueMaxSpanFlowControlTimes);
                    }
                    return;
                }

                long offset = 0L;
                try {
                    //获取mq下一次拉取偏移量
                    offset = nextPullOffset(messageQueue);
                } catch (Exception e) {
                    log.error("Failed to get next pull offset", e);
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_ON_EXCEPTION, TimeUnit.MILLISECONDS);
                    return;
                }

                if (this.isCancelled() || processQueue.isDropped()) {
                    return;
                }
                long pullDelayTimeMills = 0;
                try {
                    SubscriptionData subscriptionData;
                    //获取mq主题
                    String topic = this.messageQueue.getTopic();
                    //如果类型是订阅，就通过rebalance的订阅信息获取订阅数据，否则构建一个订阅数据
                    if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                        subscriptionData = rebalanceImpl.getSubscriptionInner().get(topic);
                    } else {
                        subscriptionData = FilterAPI.buildSubscriptionData(topic, SubscriptionData.SUB_ALL);
                    }
                    //调用拉取方法，根据mq、偏移量、拉取批次大小返回拉取结果
                    PullResult pullResult = pull(messageQueue, subscriptionData, offset, defaultLitePullConsumer.getPullBatchSize());
                    if (this.isCancelled() || processQueue.isDropped()) {
                        return;
                    }
                    //判断拉取状态
                    switch (pullResult.getPullStatus()) {
                        //如果是found，针对MQ获取对应的锁
                        case FOUND:
                            final Object objLock = messageQueueLock.fetchLockObject(messageQueue);
                            synchronized (objLock) {
                                //上锁，
                                if (pullResult.getMsgFoundList() != null && !pullResult.getMsgFoundList().isEmpty() && assignedMessageQueue.getSeekOffset(messageQueue) == -1) {
                                    //将拉取的消息列表放入pq，将消费请求放入consumeRequestCache
                                    processQueue.putMessage(pullResult.getMsgFoundList());
                                    submitConsumeRequest(new ConsumeRequest(pullResult.getMsgFoundList(), messageQueue, processQueue));
                                }
                            }
                            break;
                        case OFFSET_ILLEGAL:
                            log.warn("The pull request offset illegal, {}", pullResult.toString());
                            break;
                        default:
                            break;
                    }
                    //更新拉取偏移量
                    updatePullOffset(messageQueue, pullResult.getNextBeginOffset(), processQueue);
                } catch (Throwable e) {
                    pullDelayTimeMills = pullTimeDelayMillsWhenException;
                    log.error("An error occurred in pull message process.", e);
                }
                //如果没关闭，就提交任务
                if (!this.isCancelled()) {
                    scheduledThreadPoolExecutor.schedule(this, pullDelayTimeMills, TimeUnit.MILLISECONDS);
                } else {
                    log.warn("The Pull Task is cancelled after doPullTask, {}", messageQueue);
                }
            }
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }
    }

    private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, subscriptionData, offset, maxNums, this.defaultLitePullConsumer.getConsumerPullTimeoutMillis());
    }

    private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, true, timeout);
    }

    //ok  异步拉取mq
    private PullResult pullSyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums,
        boolean block,
        long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //异常判断
        if (null == mq) { throw new MQClientException("mq is null", null); }
        if (offset < 0) { throw new MQClientException("offset < 0", null); }
        if (maxNums <= 0) { throw new MQClientException("maxNums <= 0", null); }

        int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false, true);

        long timeoutMillis = block ? this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;
        //判断订阅类型是否是tag
        boolean isTagType = ExpressionType.isTagType(subscriptionData.getExpressionType());
        //拉取消息返回拉结果
        PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(
            mq,
            subscriptionData.getSubString(),
            subscriptionData.getExpressionType(),
            isTagType ? 0L : subscriptionData.getSubVersion(),
            offset,
            maxNums,
            sysFlag,
            0,
            this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis(),
            timeoutMillis,
            CommunicationMode.SYNC,
            null
        );
        //处理拉消息
        this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
        //执行消费消息钩子
        if (!this.consumeMessageHookList.isEmpty()) {
            ConsumeMessageContext consumeMessageContext = new ConsumeMessageContext();
            consumeMessageContext.setNamespace(defaultLitePullConsumer.getNamespace());
            consumeMessageContext.setConsumerGroup(this.groupName());
            consumeMessageContext.setMq(mq);
            consumeMessageContext.setMsgList(pullResult.getMsgFoundList());
            consumeMessageContext.setSuccess(false);
            this.executeHookBefore(consumeMessageContext);
            consumeMessageContext.setStatus(ConsumeConcurrentlyStatus.CONSUME_SUCCESS.toString());
            consumeMessageContext.setSuccess(true);
            this.executeHookAfter(consumeMessageContext);
        }
        //返回结果
        return pullResult;
    }

    private void resetTopic(List<MessageExt> msgList) {
        if (null == msgList || msgList.size() == 0) {
            return;
        }

        //If namespace not null , reset Topic without namespace.
        for (MessageExt messageExt : msgList) {
            if (null != this.defaultLitePullConsumer.getNamespace()) {
                messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.defaultLitePullConsumer.getNamespace()));
            }
        }

    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        checkServiceState();
        this.offsetStore.updateOffset(mq, offset, false);
    }

    @Override
    public String groupName() {
        return this.defaultLitePullConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultLitePullConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

        return subSet;
    }

    @Override
    public void doRebalance() {
        if (this.rebalanceImpl != null) {
            this.rebalanceImpl.doRebalance(false);
        }
    }

    @Override
    public void persistConsumerOffset() {
        try {
            checkServiceState();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            if (this.subscriptionType == SubscriptionType.SUBSCRIBE) {
                Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
                mqs.addAll(allocateMq);
            } else if (this.subscriptionType == SubscriptionType.ASSIGN) {
                Set<MessageQueue> assignedMessageQueue = this.assignedMessageQueue.getAssignedMessageQueues();
                mqs.addAll(assignedMessageQueue);
            }
            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("Persist consumer offset error for group: {} ", this.defaultLitePullConsumer.getConsumerGroup(), e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.getTopicSubscribeInfoTable().put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultLitePullConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultLitePullConsumer);
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));
        info.setProperties(prop);

        info.getSubscriptionSet().addAll(this.subscriptions());
        return info;
    }

    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        this.offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public DefaultLitePullConsumer getDefaultLitePullConsumer() {
        return defaultLitePullConsumer;
    }

    public Set<MessageQueue> fetchMessageQueues(String topic) throws MQClientException {
        checkServiceState();
        Set<MessageQueue> result = this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
        return parseMessageQueues(result);
    }

    //ok  获取主题MQ并比较
    private synchronized void fetchTopicMessageQueuesAndCompare() throws MQClientException {
        //遍历主题MQ改变监听器map
        for (Map.Entry<String, TopicMessageQueueChangeListener> entry : topicMessageQueueChangeListenerMap.entrySet()) {
            String topic = entry.getKey();
            TopicMessageQueueChangeListener topicMessageQueueChangeListener = entry.getValue();
            //获取主题的旧消息集、新消息集，判断是否发生改变
            Set<MessageQueue> oldMessageQueues = messageQueuesForTopic.get(topic);
            Set<MessageQueue> newMessageQueues = fetchMessageQueues(topic);
            boolean isChanged = !isSetEqual(newMessageQueues, oldMessageQueues);
            if (isChanged) {
                //如果改变了，就把新消息集放入map中，并触发监听器的改变方法，没实现
                messageQueuesForTopic.put(topic, newMessageQueues);
                if (topicMessageQueueChangeListener != null) {
                    topicMessageQueueChangeListener.onChanged(topic, newMessageQueues);
                }
            }
        }
    }

    private boolean isSetEqual(Set<MessageQueue> set1, Set<MessageQueue> set2) {
        if (set1 == null && set2 == null) {
            return true;
        }

        if (set1 == null || set2 == null || set1.size() != set2.size()
            || set1.size() == 0 || set2.size() == 0) {
            return false;
        }

        Iterator iter = set2.iterator();
        boolean isEqual = true;
        while (iter.hasNext()) {
            if (!set1.contains(iter.next())) {
                isEqual = false;
            }
        }
        return isEqual;
    }

    public synchronized void registerTopicMessageQueueChangeListener(String topic,
        TopicMessageQueueChangeListener listener) throws MQClientException {
        if (topic == null || listener == null) {
            throw new MQClientException("Topic or listener is null", null);
        }
        if (topicMessageQueueChangeListenerMap.containsKey(topic)) {
            log.warn("Topic {} had been registered, new listener will overwrite the old one", topic);
        }
        topicMessageQueueChangeListenerMap.put(topic, listener);
        if (this.serviceState == ServiceState.RUNNING) {
            Set<MessageQueue> messageQueues = fetchMessageQueues(topic);
            messageQueuesForTopic.put(topic, messageQueues);
        }
    }

    private Set<MessageQueue> parseMessageQueues(Set<MessageQueue> queueSet) {
        Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
        for (MessageQueue messageQueue : queueSet) {
            String userTopic = NamespaceUtil.withoutNamespace(messageQueue.getTopic(),
                this.defaultLitePullConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, messageQueue.getBrokerName(), messageQueue.getQueueId()));
        }
        return resultQueues;
    }

    public class ConsumeRequest {
        private final List<MessageExt> messageExts;
        private final MessageQueue messageQueue;
        private final ProcessQueue processQueue;

        public ConsumeRequest(final List<MessageExt> messageExts, final MessageQueue messageQueue,
            final ProcessQueue processQueue) {
            this.messageExts = messageExts;
            this.messageQueue = messageQueue;
            this.processQueue = processQueue;
        }

        public List<MessageExt> getMessageExts() {
            return messageExts;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }
}
