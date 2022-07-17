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

package org.apache.rocketmq.tools.monitor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.topic.OffsetMovedEvent;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

//ok
public class MonitorService {
    private final InternalLogger log = ClientLogger.getLog();
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("MonitorService"));

    private final MonitorConfig monitorConfig;

    private final MonitorListener monitorListener;

    private final DefaultMQAdminExt defaultMQAdminExt;
    private final DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(
        MixAll.TOOLS_CONSUMER_GROUP);
    private final DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(
        MixAll.MONITOR_CONSUMER_GROUP);

    //ok   构造方法，设置成员变量配置文件、listener、拉消费者、推消费者
    public MonitorService(MonitorConfig monitorConfig, MonitorListener monitorListener, RPCHook rpcHook) {
        this.monitorConfig = monitorConfig;
        this.monitorListener = monitorListener;

        this.defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        this.defaultMQAdminExt.setInstanceName(instanceName());
        this.defaultMQAdminExt.setNamesrvAddr(monitorConfig.getNamesrvAddr());

        this.defaultMQPullConsumer.setInstanceName(instanceName());
        this.defaultMQPullConsumer.setNamesrvAddr(monitorConfig.getNamesrvAddr());

        this.defaultMQPushConsumer.setInstanceName(instanceName());
        this.defaultMQPushConsumer.setNamesrvAddr(monitorConfig.getNamesrvAddr());
        try {
            this.defaultMQPushConsumer.setConsumeThreadMin(1);
            this.defaultMQPushConsumer.setConsumeThreadMax(1);
            this.defaultMQPushConsumer.subscribe(TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT, "*");
            //推消费者注册一个监听器，移动offset，删除消息
            this.defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {

                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                    try {
                        OffsetMovedEvent ome =
                            OffsetMovedEvent.decode(msgs.get(0).getBody(), OffsetMovedEvent.class);

                        DeleteMsgsEvent deleteMsgsEvent = new DeleteMsgsEvent();
                        deleteMsgsEvent.setOffsetMovedEvent(ome);
                        deleteMsgsEvent.setEventTimestamp(msgs.get(0).getStoreTimestamp());

                        MonitorService.this.monitorListener.reportDeleteMsgsEvent(deleteMsgsEvent);
                    } catch (Exception e) {
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
        } catch (MQClientException e) {
        }
    }

    //ok  主方法
    public static void main(String[] args) throws MQClientException {
        main0(args, null);
    }

    //ok
    public static void main0(String[] args, RPCHook rpcHook) throws MQClientException {
        //新建一个monitorService，执行其start方法
        final MonitorService monitorService =
            new MonitorService(new MonitorConfig(), new DefaultMonitorListener(), rpcHook);
        monitorService.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;

            @Override
            public void run() {
                synchronized (this) {
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        monitorService.shutdown();
                    }
                }
            }
        }, "ShutdownHook"));
    }

    //ok  实例名
    private String instanceName() {
        //系统时间+随机数+namesrv地址
        String name =
            System.currentTimeMillis() + new Random().nextInt() + this.monitorConfig.getNamesrvAddr();

        return "MonitorService_" + name.hashCode();
    }

    //ok 开启拉、推消费者，开启adminExt，开启定时任务
    public void start() throws MQClientException {
        this.defaultMQPullConsumer.start();
        this.defaultMQAdminExt.start();
        this.defaultMQPushConsumer.start();
        this.startScheduleTask();
    }

    //ok 关闭 关闭拉、推生产者，开启adminExt
    public void shutdown() {
        this.defaultMQPullConsumer.shutdown();
        this.defaultMQAdminExt.shutdown();
        this.defaultMQPushConsumer.shutdown();
    }

    //ok  开启定时任务
    private void startScheduleTask() {
        //20s初始delay，周期是monitor配置中的interval，默认60s
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    //核心方法
                    MonitorService.this.doMonitorWork();
                } catch (Exception e) {
                    log.error("doMonitorWork Exception", e);
                }
            }
        }, 1000 * 20, this.monitorConfig.getRoundInterval(), TimeUnit.MILLISECONDS);
    }

    //ok  做监控工作
    public void doMonitorWork() throws RemotingException, MQClientException, InterruptedException {
        long beginTime = System.currentTimeMillis();
        //打印日志开始
        this.monitorListener.beginRound();

        //获取所有主题列表
        TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
        //遍历所有主题列表，如果主题是以"%RETRY%"开头，就跳过前缀，获取消费者组
        for (String topic : topicList.getTopicList()) {
            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                String consumerGroup = topic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
                //上报未处理的消息和消费者组的运行信息
                try {
                    this.reportUndoneMsgs(consumerGroup);
                } catch (Exception e) {
                    // log.error("reportUndoneMsgs Exception", e);
                }

                try {
                    this.reportConsumerRunningInfo(consumerGroup);
                } catch (Exception e) {
                    // log.error("reportConsumerRunningInfo Exception", e);
                }
            }
        }
        //打印end日志
        this.monitorListener.endRound();
        //记录花费的时间
        long spentTimeMills = System.currentTimeMillis() - beginTime;
        log.info("Execute one round monitor work, spent timemills: {}", spentTimeMills);
    }

    //ok 上报未处理的消息
    private void reportUndoneMsgs(final String consumerGroup) {
        ConsumeStats cs = null;
        try {
            //检查消费者组的消费状态
            cs = defaultMQAdminExt.examineConsumeStats(consumerGroup);
        } catch (Exception e) {
            return;
        }

        //检查消费者组的连接信息，这个变量后面没用到
        ConsumerConnection cc = null;
        try {
            cc = defaultMQAdminExt.examineConsumerConnectionInfo(consumerGroup);
        } catch (Exception e) {
            return;
        }

        if (cs != null) {

            HashMap<String/* Topic */, ConsumeStats> csByTopic = new HashMap<String, ConsumeStats>();
            {
                //获取消费状态中的偏移量列表，key是消息队列，v是偏移量，遍历放入<主题 <队列，偏移量>>
                Iterator<Entry<MessageQueue, OffsetWrapper>> it = cs.getOffsetTable().entrySet().iterator();
                while (it.hasNext()) {
                    Entry<MessageQueue, OffsetWrapper> next = it.next();
                    MessageQueue mq = next.getKey();
                    OffsetWrapper ow = next.getValue();
                    ConsumeStats csTmp = csByTopic.get(mq.getTopic());
                    if (null == csTmp) {
                        csTmp = new ConsumeStats();
                        csByTopic.put(mq.getTopic(), csTmp);
                    }

                    csTmp.getOffsetTable().put(mq, ow);
                }
            }

            {
                Iterator<Entry<String, ConsumeStats>> it = csByTopic.entrySet().iterator();
                //针对每个主题都创建一个未处理消息,计算该消息，listener报告未处理消息。
                while (it.hasNext()) {
                    Entry<String, ConsumeStats> next = it.next();
                    UndoneMsgs undoneMsgs = new UndoneMsgs();
                    undoneMsgs.setConsumerGroup(consumerGroup);
                    undoneMsgs.setTopic(next.getKey());
                    //计算未处理消息
                    this.computeUndoneMsgs(undoneMsgs, next.getValue());
                    this.monitorListener.reportUndoneMsgs(undoneMsgs);
                    this.reportFailedMsgs(consumerGroup, next.getKey());
                }
            }
        }
    }

    public void reportConsumerRunningInfo(final String consumerGroup) throws InterruptedException,
        MQBrokerException, RemotingException, MQClientException {
        ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(consumerGroup);
        TreeMap<String, ConsumerRunningInfo> infoMap = new TreeMap<String, ConsumerRunningInfo>();
        for (Connection c : cc.getConnectionSet()) {
            String clientId = c.getClientId();

            if (c.getVersion() < MQVersion.Version.V3_1_8_SNAPSHOT.ordinal()) {
                continue;
            }

            try {
                ConsumerRunningInfo info =
                    defaultMQAdminExt.getConsumerRunningInfo(consumerGroup, clientId, false);
                infoMap.put(clientId, info);
            } catch (Exception e) {
            }
        }

        if (!infoMap.isEmpty()) {
            this.monitorListener.reportConsumerRunningInfo(infoMap);
        }
    }

    //ok
    private void computeUndoneMsgs(final UndoneMsgs undoneMsgs, final ConsumeStats consumeStats) {
        long total = 0;
        long singleMax = 0;
        long delayMax = 0;
        //遍历消息消费状态的偏移量列表，计算出未处理消息的三个属性：total、
        Iterator<Entry<MessageQueue, OffsetWrapper>> it = consumeStats.getOffsetTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, OffsetWrapper> next = it.next();
            MessageQueue mq = next.getKey();
            OffsetWrapper ow = next.getValue();
            long diff = ow.getBrokerOffset() - ow.getConsumerOffset();

            if (diff > singleMax) {
                singleMax = diff;
            }

            if (diff > 0) {
                total += diff;
            }

            // Delay
            if (ow.getLastTimestamp() > 0) {
                try {
                    long maxOffset = this.defaultMQPullConsumer.maxOffset(mq);
                    if (maxOffset > 0) {
                        PullResult pull = this.defaultMQPullConsumer.pull(mq, "*", maxOffset - 1, 1);
                        switch (pull.getPullStatus()) {
                            case FOUND:
                                long delay =
                                    pull.getMsgFoundList().get(0).getStoreTimestamp() - ow.getLastTimestamp();
                                if (delay > delayMax) {
                                    delayMax = delay;
                                }
                                break;
                            case NO_MATCHED_MSG:
                            case NO_NEW_MSG:
                            case OFFSET_ILLEGAL:
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                }
            }
        }

        undoneMsgs.setUndoneMsgsTotal(total);
        undoneMsgs.setUndoneMsgsSingleMQ(singleMax);
        undoneMsgs.setUndoneMsgsDelayTimeMills(delayMax);
    }

    private void reportFailedMsgs(final String consumerGroup, final String topic) {

    }
}
