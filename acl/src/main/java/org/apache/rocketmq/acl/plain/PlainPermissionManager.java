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
package org.apache.rocketmq.acl.plain;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.srvutil.FileWatchService;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
//ok  普通允许管理器
public class PlainPermissionManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    //默认普通acl文件
    private static final String DEFAULT_PLAIN_ACL_FILE = "/conf/plain_acl.yml";

    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private String fileName = System.getProperty("rocketmq.acl.plain.file", DEFAULT_PLAIN_ACL_FILE);
    //普通进入资源map，key是进入key，value是普通进入资源
    private Map<String/** AccessKey **/, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
    //全局白名单远程地址策略
    private List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

    private RemoteAddressStrategyFactory remoteAddressStrategyFactory = new RemoteAddressStrategyFactory();

    private boolean isWatchStart;

    private final DataVersion dataVersion = new DataVersion();

    //构造方法中调用load和watch方法
    public PlainPermissionManager() {
        load();
        watch();
    }

    //ok  加载
    public void load() {
        //创建普通准入资源map对象和全局白名单远程地址策略列表，load方法就是为了填充这两个对象
        Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
        List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();
        //读取配置文件获取普通acl配置数据
        JSONObject plainAclConfData = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            JSONObject.class);
        if (plainAclConfData == null || plainAclConfData.isEmpty()) {
            throw new AclException(String.format("%s file is not data", fileHome + File.separator + fileName));
        }
        log.info("Broker plain acl conf data is : ", plainAclConfData.toString());
        //从配置文件获取全局白名单远程地址列表，根据地址给策略列表中添加策略
        JSONArray globalWhiteRemoteAddressesList = plainAclConfData.getJSONArray("globalWhiteRemoteAddresses");
        if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
            for (int i = 0; i < globalWhiteRemoteAddressesList.size(); i++) {
                globalWhiteRemoteAddressStrategy.add(remoteAddressStrategyFactory.
                    getRemoteAddressStrategy(globalWhiteRemoteAddressesList.getString(i)));
            }
        }
        //从配置文件获取所有账户，根据账户构造进入资源，放入普通准入资源map中
        JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
        if (accounts != null && !accounts.isEmpty()) {
            List<PlainAccessConfig> plainAccessConfigList = accounts.toJavaList(PlainAccessConfig.class);
            for (PlainAccessConfig plainAccessConfig : plainAccessConfigList) {
                PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                plainAccessResourceMap.put(plainAccessResource.getAccessKey(), plainAccessResource);
            }
        }
        // For loading dataversion part just   从配置文件获取数据版本列表，根据列表第一项设置dataVersion
        JSONArray tempDataVersion = plainAclConfData.getJSONArray(AclConstants.CONFIG_DATA_VERSION);
        if (tempDataVersion != null && !tempDataVersion.isEmpty()) {
            List<DataVersion> dataVersion = tempDataVersion.toJavaList(DataVersion.class);
            DataVersion firstElement = dataVersion.get(0);
            this.dataVersion.assignNewOne(firstElement);
        }
        this.globalWhiteRemoteAddressStrategy = globalWhiteRemoteAddressStrategy;
        this.plainAccessResourceMap = plainAccessResourceMap;
    }

    public String getAclConfigDataVersion() {
        return this.dataVersion.toJson();
    }

    //ok  更新acl配置文件版本
    private Map<String, Object> updateAclConfigFileVersion(Map<String, Object> updateAclConfigMap) {

        dataVersion.nextVersion();
        List<Map<String, Object>> versionElement = new ArrayList<Map<String, Object>>();
        Map<String, Object> accountsMap = new LinkedHashMap<String, Object>() {
            {
                put(AclConstants.CONFIG_COUNTER, dataVersion.getCounter().longValue());
                put(AclConstants.CONFIG_TIME_STAMP, dataVersion.getTimestamp());
            }
        };
        versionElement.add(accountsMap);
        updateAclConfigMap.put(AclConstants.CONFIG_DATA_VERSION, versionElement);
        return updateAclConfigMap;
    }

    //ok  更新准入配置账户
    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {
        if (plainAccessConfig == null) {
            log.error("Parameter value plainAccessConfig is null,Please check your parameter");
            throw new AclException("Parameter value plainAccessConfig is null, Please check your parameter");
        }
        //检查普通准入配置是否正常
        checkPlainAccessConfig(plainAccessConfig);
        //检查主题和组的资源参数
        Permission.checkResourcePerms(plainAccessConfig.getTopicPerms());
        Permission.checkResourcePerms(plainAccessConfig.getGroupPerms());
        //获取acl配置文件
        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            Map.class);
        if (aclAccessConfigMap == null || aclAccessConfigMap.isEmpty()) {
            throw new AclException(String.format("the %s file is not found or empty", fileHome + File.separator + fileName));
        }
        //从配置文件获取账户
        List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
        Map<String, Object> updateAccountMap = null;
        if (accounts != null) {
            //遍历账户，如果账户的准入key和入参的key相同，就更新该账户并落盘
            for (Map<String, Object> account : accounts) {
                if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(plainAccessConfig.getAccessKey())) {
                    // Update acl access config elements
                    accounts.remove(account);
                    updateAccountMap = createAclAccessConfigMap(account, plainAccessConfig);
                    accounts.add(updateAccountMap);
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
                    if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                        return true;
                    }
                    return false;
                }
            }
            // Create acl access config elements
            //上面没找到相同准入key的账户，就新建一个并放入账户列表然后落盘
            accounts.add(createAclAccessConfigMap(null, plainAccessConfig));
            aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
            if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                return true;
            }
            return false;
        }
        log.error("Users must ensure that the acl yaml config file has accounts node element");
        return false;
    }

    //ok  创建acl准入配置文件，方法名有待商榷，如果入参existedAccountMap不为空，就是修改这个账户，如果入参为空，才是创建账户
    private Map<String, Object> createAclAccessConfigMap(Map<String, Object> existedAccountMap,
        PlainAccessConfig plainAccessConfig) {
        //构建新账户map，如果入参existedAccountMap不为空，就是修改这个账户，如果入参为空，才是创建账户
        Map<String, Object> newAccountsMap = null;
        if (existedAccountMap == null) {
            newAccountsMap = new LinkedHashMap<String, Object>();
        } else {
            newAccountsMap = existedAccountMap;
        }
        //入参非空判断
        if (StringUtils.isEmpty(plainAccessConfig.getAccessKey()) ||
            plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH) {
            throw new AclException(String.format(
                "The accessKey=%s cannot be null and length should longer than 6",
                plainAccessConfig.getAccessKey()));
        }
        //更新账户的一系列属性
        newAccountsMap.put(AclConstants.CONFIG_ACCESS_KEY, plainAccessConfig.getAccessKey());

        if (!StringUtils.isEmpty(plainAccessConfig.getSecretKey())) {
            if (plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
                throw new AclException(String.format(
                    "The secretKey=%s value length should longer than 6",
                    plainAccessConfig.getSecretKey()));
            }
            newAccountsMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig.getSecretKey());
        }
        if (plainAccessConfig.getWhiteRemoteAddress() != null) {
            newAccountsMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig.getWhiteRemoteAddress());
        }
        if (!StringUtils.isEmpty(String.valueOf(plainAccessConfig.isAdmin()))) {
            newAccountsMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig.isAdmin());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultTopicPerm())) {
            newAccountsMap.put(AclConstants.CONFIG_DEFAULT_TOPIC_PERM, plainAccessConfig.getDefaultTopicPerm());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultGroupPerm())) {
            newAccountsMap.put(AclConstants.CONFIG_DEFAULT_GROUP_PERM, plainAccessConfig.getDefaultGroupPerm());
        }
        if (plainAccessConfig.getTopicPerms() != null) {
            newAccountsMap.put(AclConstants.CONFIG_TOPIC_PERMS, plainAccessConfig.getTopicPerms());
        }
        if (plainAccessConfig.getGroupPerms() != null) {
            newAccountsMap.put(AclConstants.CONFIG_GROUP_PERMS, plainAccessConfig.getGroupPerms());
        }

        return newAccountsMap;
    }

    //删除某个准入key的准入配置，删除成功返回true，删除失败或者找不到这个账户都返回false
    public boolean deleteAccessConfig(String accesskey) {
        if (StringUtils.isEmpty(accesskey)) {
            log.error("Parameter value accesskey is null or empty String,Please check your parameter");
            return false;
        }
        //读取acl配置文件
        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            Map.class);
        if (aclAccessConfigMap == null || aclAccessConfigMap.isEmpty()) {
            throw new AclException(String.format("the %s file is not found or empty", fileHome + File.separator + fileName));
        }
        List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get("accounts");
        if (accounts != null) {
            //遍历账户，找到和入参准入key相同的账户，删除并落盘
            Iterator<Map<String, Object>> itemIterator = accounts.iterator();
            while (itemIterator.hasNext()) {
                if (itemIterator.next().get(AclConstants.CONFIG_ACCESS_KEY).equals(accesskey)) {
                    // Delete the related acl config element
                    itemIterator.remove();
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
                    if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                        return true;
                    }
                    return false;
                }
            }
        }
        log.error("Users must ensure that the acl yaml config file has related acl config elements");
        return false;
    }

    //ok  更新全局白名单配置
    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList) {
        //读取配置文件
        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            Map.class);
        if (aclAccessConfigMap == null || aclAccessConfigMap.isEmpty()) {
            throw new AclException(String.format("the %s file is not found or empty", fileHome + File.separator + fileName));
        }
        //从配置文件读取全局白名单地址列表
        List<String> globalWhiteRemoteAddrList = (List<String>) aclAccessConfigMap.get(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
        if (globalWhiteRemoteAddrList != null) {
            //先清空，然后加入入参的白名单列表并落盘
            globalWhiteRemoteAddrList.clear();
            if (globalWhiteAddrsList != null) {
                globalWhiteRemoteAddrList.addAll(globalWhiteAddrsList);
            }
            // Update globalWhiteRemoteAddr element in memory map firstly
            aclAccessConfigMap.put(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS, globalWhiteRemoteAddrList);
            if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                return true;
            }
            return false;
        }
        log.error("Users must ensure that the acl yaml config file has globalWhiteRemoteAddresses flag firstly");
        return false;
    }

    //ok  获取所有acl配置，包括白名单地址列表和所有账户
    public AclConfig getAllAclConfig() {
        AclConfig aclConfig = new AclConfig();
        List<PlainAccessConfig> configs = new ArrayList<>();
        List<String> whiteAddrs = new ArrayList<>();
        //读取acl配置文件
        JSONObject plainAclConfData = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            JSONObject.class);
        if (plainAclConfData == null || plainAclConfData.isEmpty()) {
            throw new AclException(String.format("%s file is not data", fileHome + File.separator + fileName));
        }
        //从配置中读取全局白名单地址列表
        JSONArray globalWhiteAddrs = plainAclConfData.getJSONArray(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
        if (globalWhiteAddrs != null && !globalWhiteAddrs.isEmpty()) {
            whiteAddrs = globalWhiteAddrs.toJavaList(String.class);
        }
        //读取所有账户
        JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
        if (accounts != null && !accounts.isEmpty()) {
            configs = accounts.toJavaList(PlainAccessConfig.class);
        }
        aclConfig.setGlobalWhiteAddrs(whiteAddrs);
        aclConfig.setPlainAccessConfigs(configs);
        return aclConfig;
    }

    //ok  监听
    private void watch() {
        try {
            //创建一个监听服务线程类，监听acl配置文件路径，文件一旦改变就重新加载
            String watchFilePath = fileHome + fileName;
            FileWatchService fileWatchService = new FileWatchService(new String[] {watchFilePath}, new FileWatchService.Listener() {
                @Override
                public void onChanged(String path) {
                    log.info("The plain acl yml changed, reload the context");
                    load();
                }
            });
            //开启监听，记录标志位
            fileWatchService.start();
            log.info("Succeed to start AclWatcherService");
            this.isWatchStart = true;
        } catch (Exception e) {
            log.error("Failed to start AclWatcherService", e);
        }
    }

    //ok  检查参数
    void checkPerm(PlainAccessResource needCheckedAccess, PlainAccessResource ownedAccess) {
        //需要检查的准入资源要有admin的code
        if (Permission.needAdminPerm(needCheckedAccess.getRequestCode()) && !ownedAccess.isAdmin()) {
            throw new AclException(String.format("Need admin permission for request code=%d, but accessKey=%s is not", needCheckedAccess.getRequestCode(), ownedAccess.getAccessKey()));
        }
        //获取需要检查的准入参数表和已有的准入参数表
        Map<String, Byte> needCheckedPermMap = needCheckedAccess.getResourcePermMap();
        Map<String, Byte> ownedPermMap = ownedAccess.getResourcePermMap();

        if (needCheckedPermMap == null) {
            // If the needCheckedPermMap is null,then return
            return;
        }
        //如果已有的为空并且已有的是admin就返回
        if (ownedPermMap == null && ownedAccess.isAdmin()) {
            // If the ownedPermMap is null and it is an admin user, then return
            return;
        }

        for (Map.Entry<String, Byte> needCheckedEntry : needCheckedPermMap.entrySet()) {
            //遍历需要检查的参数表
            String resource = needCheckedEntry.getKey();
            Byte neededPerm = needCheckedEntry.getValue();
            //判断资源是否是重试主题
            boolean isGroup = PlainAccessResource.isRetryTopic(resource);
            //如果已有的参数表为空或者已有参数表不包含某参数
            if (ownedPermMap == null || !ownedPermMap.containsKey(resource)) {
                // Check the default perm
                byte ownedPerm = isGroup ? ownedAccess.getDefaultGroupPerm() :
                    ownedAccess.getDefaultTopicPerm();
                //检查参数，如果没通过检查就抛异常
                if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                    throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
                }
                continue;
            }
            if (!Permission.checkPermission(neededPerm, ownedPermMap.get(resource))) {
                throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
            }
        }
    }

    //ok  清除准入信息
    void clearPermissionInfo() {
        this.plainAccessResourceMap.clear();
        this.globalWhiteRemoteAddressStrategy.clear();
    }

    //ok  检查普通准入配置，进入key和加密key不能为空，长度不能过长
    public void checkPlainAccessConfig(PlainAccessConfig plainAccessConfig) throws AclException {
        if (plainAccessConfig.getAccessKey() == null
                || plainAccessConfig.getSecretKey() == null
                || plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH
                || plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
            throw new AclException(String.format(
                    "The accessKey=%s and secretKey=%s cannot be null and length should longer than 6",
                    plainAccessConfig.getAccessKey(), plainAccessConfig.getSecretKey()));
        }
    }

    //ok  用普通准入配置构建准入资源
    public PlainAccessResource buildPlainAccessResource(PlainAccessConfig plainAccessConfig) throws AclException {
        checkPlainAccessConfig(plainAccessConfig);
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setAccessKey(plainAccessConfig.getAccessKey());
        plainAccessResource.setSecretKey(plainAccessConfig.getSecretKey());
        plainAccessResource.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());
        plainAccessResource.setAdmin(plainAccessConfig.isAdmin());
        plainAccessResource.setDefaultGroupPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultGroupPerm()));
        plainAccessResource.setDefaultTopicPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultTopicPerm()));
        Permission.parseResourcePerms(plainAccessResource, false, plainAccessConfig.getGroupPerms());
        Permission.parseResourcePerms(plainAccessResource, true, plainAccessConfig.getTopicPerms());
        plainAccessResource.setRemoteAddressStrategy(remoteAddressStrategyFactory.
            getRemoteAddressStrategy(plainAccessResource.getWhiteRemoteAddress()));
        return plainAccessResource;
    }

    //ok  校验某个普通准入资源
    public void validate(PlainAccessResource plainAccessResource) {

        // Check the global white remote addr
        for (RemoteAddressStrategy remoteAddressStrategy : globalWhiteRemoteAddressStrategy) {
            if (remoteAddressStrategy.match(plainAccessResource)) {
                return;
            }
        }
        if (plainAccessResource.getAccessKey() == null) {
            throw new AclException(String.format("No accessKey is configured"));
        }
        if (!plainAccessResourceMap.containsKey(plainAccessResource.getAccessKey())) {
            throw new AclException(String.format("No acl config for %s", plainAccessResource.getAccessKey()));
        }
        // Check the white addr for accesskey
        PlainAccessResource ownedAccess = plainAccessResourceMap.get(plainAccessResource.getAccessKey());
        if (ownedAccess.getRemoteAddressStrategy().match(plainAccessResource)) {
            return;
        }
        // Check the signature
        String signature = AclUtils.calSignature(plainAccessResource.getContent(), ownedAccess.getSecretKey());
        if (!signature.equals(plainAccessResource.getSignature())) {
            throw new AclException(String.format("Check signature failed for accessKey=%s", plainAccessResource.getAccessKey()));
        }
        // Check perm of each resource
        checkPerm(plainAccessResource, ownedAccess);
    }

    public boolean isWatchStart() {
        return isWatchStart;
    }
}
