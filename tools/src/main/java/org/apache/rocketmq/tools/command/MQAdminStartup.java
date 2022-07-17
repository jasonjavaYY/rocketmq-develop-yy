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
package org.apache.rocketmq.tools.command;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.acl.ClusterAclConfigVersionListSubCommand;
import org.apache.rocketmq.tools.command.acl.GetAccessConfigSubCommand;
import org.apache.rocketmq.tools.command.acl.DeleteAccessConfigSubCommand;
import org.apache.rocketmq.tools.command.acl.UpdateAccessConfigSubCommand;
import org.apache.rocketmq.tools.command.acl.UpdateGlobalWhiteAddrSubCommand;
import org.apache.rocketmq.tools.command.broker.BrokerConsumeStatsSubCommad;
import org.apache.rocketmq.tools.command.broker.BrokerStatusSubCommand;
import org.apache.rocketmq.tools.command.broker.CleanExpiredCQSubCommand;
import org.apache.rocketmq.tools.command.broker.CleanUnusedTopicCommand;
import org.apache.rocketmq.tools.command.broker.GetBrokerConfigCommand;
import org.apache.rocketmq.tools.command.broker.SendMsgStatusCommand;
import org.apache.rocketmq.tools.command.broker.UpdateBrokerConfigSubCommand;
import org.apache.rocketmq.tools.command.cluster.CLusterSendMsgRTCommand;
import org.apache.rocketmq.tools.command.cluster.ClusterListSubCommand;
import org.apache.rocketmq.tools.command.connection.ConsumerConnectionSubCommand;
import org.apache.rocketmq.tools.command.connection.ProducerConnectionSubCommand;
import org.apache.rocketmq.tools.command.consumer.ConsumerProgressSubCommand;
import org.apache.rocketmq.tools.command.consumer.ConsumerStatusSubCommand;
import org.apache.rocketmq.tools.command.consumer.DeleteSubscriptionGroupCommand;
import org.apache.rocketmq.tools.command.consumer.GetConsumerConfigSubCommand;
import org.apache.rocketmq.tools.command.consumer.StartMonitoringSubCommand;
import org.apache.rocketmq.tools.command.consumer.UpdateSubGroupSubCommand;
import org.apache.rocketmq.tools.command.export.ExportMetricsCommand;
import org.apache.rocketmq.tools.command.export.ExportConfigsCommand;
import org.apache.rocketmq.tools.command.message.CheckMsgSendRTCommand;
import org.apache.rocketmq.tools.command.message.ConsumeMessageCommand;
import org.apache.rocketmq.tools.command.message.PrintMessageByQueueCommand;
import org.apache.rocketmq.tools.command.message.PrintMessageSubCommand;
import org.apache.rocketmq.tools.command.message.QueryMsgByIdSubCommand;
import org.apache.rocketmq.tools.command.message.QueryMsgByKeySubCommand;
import org.apache.rocketmq.tools.command.message.QueryMsgByOffsetSubCommand;
import org.apache.rocketmq.tools.command.message.QueryMsgByUniqueKeySubCommand;
import org.apache.rocketmq.tools.command.message.QueryMsgTraceByIdSubCommand;
import org.apache.rocketmq.tools.command.message.SendMessageCommand;
import org.apache.rocketmq.tools.command.namesrv.AddWritePermSubCommand;
import org.apache.rocketmq.tools.command.namesrv.DeleteKvConfigCommand;
import org.apache.rocketmq.tools.command.namesrv.GetNamesrvConfigCommand;
import org.apache.rocketmq.tools.command.namesrv.UpdateKvConfigCommand;
import org.apache.rocketmq.tools.command.namesrv.UpdateNamesrvConfigCommand;
import org.apache.rocketmq.tools.command.namesrv.WipeWritePermSubCommand;
import org.apache.rocketmq.tools.command.offset.CloneGroupOffsetCommand;
import org.apache.rocketmq.tools.command.offset.ResetOffsetByTimeCommand;
import org.apache.rocketmq.tools.command.offset.SkipAccumulationSubCommand;
import org.apache.rocketmq.tools.command.queue.QueryConsumeQueueCommand;
import org.apache.rocketmq.tools.command.stats.StatsAllSubCommand;
import org.apache.rocketmq.tools.command.topic.AllocateMQSubCommand;
import org.apache.rocketmq.tools.command.topic.DeleteTopicSubCommand;
import org.apache.rocketmq.tools.command.export.ExportMetadataCommand;
import org.apache.rocketmq.tools.command.topic.TopicClusterSubCommand;
import org.apache.rocketmq.tools.command.topic.TopicListSubCommand;
import org.apache.rocketmq.tools.command.topic.TopicRouteSubCommand;
import org.apache.rocketmq.tools.command.topic.TopicStatusSubCommand;
import org.apache.rocketmq.tools.command.topic.UpdateOrderConfCommand;
import org.apache.rocketmq.tools.command.topic.UpdateTopicPermSubCommand;
import org.apache.rocketmq.tools.command.topic.UpdateTopicSubCommand;
import org.slf4j.LoggerFactory;

public class MQAdminStartup {
    protected static List<SubCommand> subCommandList = new ArrayList<SubCommand>();
    private static String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    public static void main(String[] args) {
        main0(args, null);
    }

    public static void main0(String[] args, RPCHook rpcHook) {
        //设置底层传输模块版本为当前版本
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();
        //初始化子命令模块，构造所有子命令对象并加入列表subCommandList
        initCommand();
        try {
            //初始化日志
            initLogback();
            switch (args.length) {
                //如果没有入参，打印帮助信息提示
                case 0:
                    printHelp();
                    break;
                    //如果是2个参数，第一个是help，就根据第二个参数找到对应的子命令对象
                case 2:
                    if (args[0].equals("help")) {
                        SubCommand cmd = findSubCommand(args[1]);
                        if (cmd != null) {
                            //如果子命令对象存在，就构造一个help和namesrv的ops，ops代表该子命令对象支持哪些参数
                            Options options = ServerUtil.buildCommandlineOptions(new Options());
                            //调用该类子命令的buildLine，将该类子命令支持的所有tag写入ops
                            options = cmd.buildCommandlineOptions(options);
                            if (options != null) {
                                //然后打印帮助命令
                                ServerUtil.printCommandLineHelp("mqadmin " + cmd.commandName(), options);
                            }
                        } else {
                            //找不到子命令对象，就是子命令不支持
                            System.out.printf("The sub command %s not exist.%n", args[1]);
                        }
                        break;
                    }
                case 1:
                default:
                    //一个参数，或者多个参数，或者2个参数但第一个参数不是help
                    //根据第一个参数找到子命令对象
                    SubCommand cmd = findSubCommand(args[0]);
                    if (cmd != null) {
                        //如果找到了子模块，就去掉第一个参数获取子参数列表
                        String[] subargs = parseSubArgs(args);

                        //构造一个help和namesrv的ops
                        Options options = ServerUtil.buildCommandlineOptions(new Options());
                        //将子命令对象支持的参数类型都加入ops，根据传入的参数和ops解析得到命令行对象
                        final CommandLine commandLine =
                            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options),
                                new PosixParser());
                        //如果解析的命令行对象为空，直接返回
                        if (null == commandLine) {
                            return;
                        }

                        //如果命令行对象有n这个option，就获取namesrv地址，把地址加入系统参数
                        if (commandLine.hasOption('n')) {
                            String namesrvAddr = commandLine.getOptionValue('n');
                            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
                        }
                        //核心，调用具体的子模块的执行逻辑执行命令行
                        cmd.execute(commandLine, options, AclUtils.getAclRPCHook(rocketmqHome + MixAll.ACL_CONF_TOOLS_FILE));
                    } else {
                        //如果子命令模块为空，就打印没找到
                        System.out.printf("The sub command %s not exist.%n", args[0]);
                    }
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //初始化命令，将所有命令模块加入到模块列表
    public static void initCommand() {
        initCommand(new UpdateTopicSubCommand());
        initCommand(new DeleteTopicSubCommand());
        initCommand(new UpdateSubGroupSubCommand());
        initCommand(new DeleteSubscriptionGroupCommand());
        initCommand(new UpdateBrokerConfigSubCommand());
        initCommand(new UpdateTopicPermSubCommand());

        initCommand(new TopicRouteSubCommand());
        initCommand(new TopicStatusSubCommand());
        initCommand(new TopicClusterSubCommand());

        initCommand(new BrokerStatusSubCommand());
        initCommand(new QueryMsgByIdSubCommand());
        initCommand(new QueryMsgByKeySubCommand());
        initCommand(new QueryMsgByUniqueKeySubCommand());
        initCommand(new QueryMsgByOffsetSubCommand());
        initCommand(new QueryMsgTraceByIdSubCommand());

        initCommand(new PrintMessageSubCommand());
        initCommand(new PrintMessageByQueueCommand());
        initCommand(new SendMsgStatusCommand());
        initCommand(new BrokerConsumeStatsSubCommad());

        initCommand(new ProducerConnectionSubCommand());
        initCommand(new ConsumerConnectionSubCommand());
        initCommand(new ConsumerProgressSubCommand());
        initCommand(new ConsumerStatusSubCommand());
        initCommand(new CloneGroupOffsetCommand());

        initCommand(new ClusterListSubCommand());
        initCommand(new TopicListSubCommand());

        initCommand(new UpdateKvConfigCommand());
        initCommand(new DeleteKvConfigCommand());

        initCommand(new WipeWritePermSubCommand());
        initCommand(new AddWritePermSubCommand());
        initCommand(new ResetOffsetByTimeCommand());
        initCommand(new SkipAccumulationSubCommand());

        initCommand(new UpdateOrderConfCommand());
        initCommand(new CleanExpiredCQSubCommand());
        initCommand(new CleanUnusedTopicCommand());

        initCommand(new StartMonitoringSubCommand());
        initCommand(new StatsAllSubCommand());

        initCommand(new AllocateMQSubCommand());

        initCommand(new CheckMsgSendRTCommand());
        initCommand(new CLusterSendMsgRTCommand());

        initCommand(new GetNamesrvConfigCommand());
        initCommand(new UpdateNamesrvConfigCommand());
        initCommand(new GetBrokerConfigCommand());
        initCommand(new GetConsumerConfigSubCommand());

        initCommand(new QueryConsumeQueueCommand());
        initCommand(new SendMessageCommand());
        initCommand(new ConsumeMessageCommand());

        //for acl command
        initCommand(new UpdateAccessConfigSubCommand());
        initCommand(new DeleteAccessConfigSubCommand());
        initCommand(new ClusterAclConfigVersionListSubCommand());
        initCommand(new UpdateGlobalWhiteAddrSubCommand());
        initCommand(new GetAccessConfigSubCommand());

        initCommand(new ExportMetadataCommand());
        initCommand(new ExportConfigsCommand());
        initCommand(new ExportMetricsCommand());
    }

    //ok  初始化日志logback
    private static void initLogback() throws JoranException {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(rocketmqHome + "/conf/logback_tools.xml");
    }

    //ok  打印整体帮助信息，子命令的名字和描述
    private static void printHelp() {
        System.out.printf("The most commonly used mqadmin commands are:%n");
        for (SubCommand cmd : subCommandList) {
            System.out.printf("   %-20s %s%n", cmd.commandName(), cmd.commandDesc());
        }

        System.out.printf("%nSee 'mqadmin help <command>' for more information on a specific command.%n");
    }

    //ok  根据命令名找到对应的子命令类对象
    private static SubCommand findSubCommand(final String name) {
        for (SubCommand cmd : subCommandList) {
            if (cmd.commandName().toUpperCase().equals(name.toUpperCase())) {
                return cmd;
            }
        }

        return null;
    }

    //ok  解析子参数，返回去除首个参数后的剩余参数列表
    private static String[] parseSubArgs(String[] args) {
        if (args.length > 1) {
            String[] result = new String[args.length - 1];
            for (int i = 0; i < args.length - 1; i++) {
                result[i] = args[i + 1];
            }
            return result;
        }
        return null;
    }

    //ok  将子命令类加入到列表
    public static void initCommand(SubCommand command) {
        subCommandList.add(command);
    }
}
