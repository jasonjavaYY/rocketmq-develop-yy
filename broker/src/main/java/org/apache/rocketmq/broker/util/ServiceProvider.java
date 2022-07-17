/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to
 * You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.rocketmq.broker.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ServiceProvider {

    private final static Logger LOG = LoggerFactory.getLogger(ServiceProvider.class);
    /**
     * A reference to the classloader that loaded this class. It's more efficient to compute it once and cache it here.
     */
    private static ClassLoader thisClassLoader;

    /**
     * JDK1.3+ <a href= "http://java.sun.com/j2se/1.3/docs/guide/jar/jar.html#Service%20Provider" > 'Service Provider' specification</a>.
     */
    public static final String TRANSACTION_SERVICE_ID = "META-INF/service/org.apache.rocketmq.broker.transaction.TransactionalMessageService";
    public static final String TRANSACTION_LISTENER_ID = "META-INF/service/org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener";
    public static final String RPC_HOOK_ID = "META-INF/service/org.apache.rocketmq.remoting.RPCHook";
    public static final String ACL_VALIDATOR_ID = "META-INF/service/org.apache.rocketmq.acl.AccessValidator";

    static {
        thisClassLoader = getClassLoader(ServiceProvider.class);
    }

    /**
     * Returns a string that uniquely identifies the specified object, including its class.
     * <p>
     * The returned string is of form "classname@hashcode", ie is the same as the return value of the Object.toString() method, but works even when the specified object's class has overidden the toString method.
     *
     * @param o may be null.
     * @return a string of form classname@hashcode, or "null" if param o is null.
     */
    //对象id方法
    protected static String objectId(Object o) {
        if (o == null) {  //如果对象为空，返回null
            return "null";
        } else {  // 否则返回对象类名@对象哈希码
            return o.getClass().getName() + "@" + System.identityHashCode(o);
        }
    }

    //ok 获取类加载器
    protected static ClassLoader getClassLoader(Class<?> clazz) {
        try {
            return clazz.getClassLoader();  //返回某个类的加载器
        } catch (SecurityException e) {
            LOG.error("Unable to get classloader for class {} due to security restrictions !",
                clazz, e.getMessage());
            throw e;
        }
    }

    //ok  获取上下文类加载器
    protected static ClassLoader getContextClassLoader() {
        ClassLoader classLoader = null;
        try {  //返回当前线程的类加载器
            classLoader = Thread.currentThread().getContextClassLoader();
        } catch (SecurityException ex) {
            /**
             * The getContextClassLoader() method throws SecurityException when the context
             * class loader isn't an ancestor of the calling class's class
             * loader, or if security permissions are restricted.
             */
        }
        return classLoader;
    }

    //获取资源流
    protected static InputStream getResourceAsStream(ClassLoader loader, String name) {
        if (loader != null) {
            return loader.getResourceAsStream(name);  //返回类加载器的资源流
        } else {
            return ClassLoader.getSystemResourceAsStream(name);
        }
    }

    public static <T> List<T> load(String name, Class<?> clazz) {
        LOG.info("Looking for a resource file of name [{}] ...", name);
        List<T> services = new ArrayList<T>();
        try {
            ArrayList<String> names = new ArrayList<String>();
            //获取当前线程类加载器的指定name的资源流
            final InputStream is = getResourceAsStream(getContextClassLoader(), name);
            if (is != null) {
                BufferedReader reader;
                try {  //将输入流转换为reader
                    reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                } catch (java.io.UnsupportedEncodingException e) {
                    reader = new BufferedReader(new InputStreamReader(is));
                }
                String serviceName = reader.readLine();  //获取服务名，将服务名加入列表
                while (serviceName != null && !"".equals(serviceName)) {
                    LOG.info(
                        "Creating an instance as specified by file {} which was present in the path of the context classloader.",
                        name);
                    if (!names.contains(serviceName)) {
                        names.add(serviceName);
                    }
                    services.add((T)initService(getContextClassLoader(), serviceName, clazz));  //根据上下文类加载器和服务名及类型初始化类型
                    serviceName = reader.readLine();  //获取下一个服务名
                }
                reader.close();
            } else {
                // is == null
                LOG.warn("No resource file with name [{}] found.", name);
            }
        } catch (Exception e) {
            LOG.error("Error occured when looking for resource file " + name, e);
        }
        return services;  //返回所有
    }

    public static <T> T loadClass(String name, Class<?> clazz) {
        final InputStream is = getResourceAsStream(getContextClassLoader(), name);
        if (is != null) {
            BufferedReader reader;
            try {
                try {
                    reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                } catch (java.io.UnsupportedEncodingException e) {
                    reader = new BufferedReader(new InputStreamReader(is));
                }
                String serviceName = reader.readLine();
                reader.close();
                if (serviceName != null && !"".equals(serviceName)) {
                    return initService(getContextClassLoader(), serviceName, clazz);
                } else {
                    LOG.warn("ServiceName is empty!");
                    return null;
                }
            } catch (Exception e) {
                LOG.warn("Error occurred when looking for resource file " + name, e);
            }
        }
        return null;
    }

    protected static <T> T initService(ClassLoader classLoader, String serviceName, Class<?> clazz) {
        Class<?> serviceClazz = null;
        try {
            if (classLoader != null) {
                try {
                    // Warning: must typecast here & allow exception to be generated/caught & recast properly
                    serviceClazz = classLoader.loadClass(serviceName);
                    if (clazz.isAssignableFrom(serviceClazz)) {
                        LOG.info("Loaded class {} from classloader {}", serviceClazz.getName(),
                            objectId(classLoader));
                    } else {
                        // This indicates a problem with the ClassLoader tree. An incompatible ClassLoader was used to load the implementation.
                        LOG.error(
                            "Class {} loaded from classloader {} does not extend {} as loaded by this classloader.",
                            new Object[] {serviceClazz.getName(),
                                objectId(serviceClazz.getClassLoader()), clazz.getName()});
                    }
                    return (T)serviceClazz.newInstance();
                } catch (ClassNotFoundException ex) {
                    if (classLoader == thisClassLoader) {
                        // Nothing more to try, onwards.
                        LOG.warn("Unable to locate any class {} via classloader", serviceName,
                            objectId(classLoader));
                        throw ex;
                    }
                    // Ignore exception, continue
                } catch (NoClassDefFoundError e) {
                    if (classLoader == thisClassLoader) {
                        // Nothing more to try, onwards.
                        LOG.warn(
                            "Class {} cannot be loaded via classloader {}.it depends on some other class that cannot be found.",
                            serviceClazz, objectId(classLoader));
                        throw e;
                    }
                    // Ignore exception, continue
                }
            }
        } catch (Exception e) {
            LOG.error("Unable to init service.", e);
        }
        return (T)serviceClazz;
    }
}
