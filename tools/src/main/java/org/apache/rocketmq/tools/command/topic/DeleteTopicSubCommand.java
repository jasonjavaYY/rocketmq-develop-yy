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
package org.apache.rocketmq.tools.command.topic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

//ok  删除主题子命令类
public class DeleteTopicSubCommand implements SubCommand {
    //ok  删除主题
    public static void deleteTopic(final DefaultMQAdminExt adminExt,
        final String clusterName,
        final String topic
    ) throws InterruptedException, MQBrokerException, RemotingException, MQClientException {

        //根据集群名匹配主从地址
        Set<String> brokerAddressSet = CommandUtil.fetchMasterAndSlaveAddrByClusterName(adminExt, clusterName);
        //根据broker地址列表和主题，执行删除主题操作，打印删除成功，因为是admin脚本操作，所以直接sout控制台输出，不是日志输出
        adminExt.deleteTopicInBroker(brokerAddressSet, topic);
        System.out.printf("delete topic [%s] from cluster [%s] success.%n", topic, clusterName);

        //用admin获取namesrv地址列表，如果能获取到，就删除所有namesrv中的这个主题信息
        Set<String> nameServerSet = null;
        if (adminExt.getNamesrvAddr() != null) {
            String[] ns = adminExt.getNamesrvAddr().trim().split(";");
            nameServerSet = new HashSet(Arrays.asList(ns));
        }
        adminExt.deleteTopicInNameServer(nameServerSet, topic);
        System.out.printf("delete topic [%s] from NameServer success.%n", topic);
    }

    @Override
    public String commandName() {
        return "deleteTopic";
    }

    @Override
    public String commandDesc() {
        return "Delete topic from broker and NameServer.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "delete topic from which cluster");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        //根据钩子函数创建一个admin，设置名字为系统时间
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            //获取主题
            String topic = commandLine.getOptionValue('t').trim();

            //如果命令包含c选项，说明有集群名，就获取集群名
            if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                //启动admin
                adminExt.start();
                //针对某集群某主题，执行admin删除
                deleteTopic(adminExt, clusterName, topic);
                return;
            }
            //如果没有集群信息，就打印帮助信息
            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            adminExt.shutdown();
        }
    }
}
