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

package org.apache.rocketmq.client.latency;

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
//ok  只有这一个实现
public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {
    //存放故障项列表
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<String, FaultItem>(16);
    //哪一项故障最糟糕
    private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

    //ok  更新故障项，三个参数是broker名，当前延迟，不可用持续时间
    @Override
    public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration) {
        //根据broker名获取旧的故障项
        FaultItem old = this.faultItemTable.get(name);
        if (null == old) {
            //如果旧故障项为空，就新建一个并更新属性，开始时间为当前时间+不可用持续时间
            final FaultItem faultItem = new FaultItem(name);
            faultItem.setCurrentLatency(currentLatency);
            faultItem.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
            //把故障项放入列表
            old = this.faultItemTable.putIfAbsent(name, faultItem);
            if (old != null) {
                old.setCurrentLatency(currentLatency);
                old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
            }
        } else {
            //否则直接更新故障项的当前延迟和开始时间
            old.setCurrentLatency(currentLatency);
            old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
        }
    }

    //判断某个broker的故障项是否可用
    @Override
    public boolean isAvailable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isAvailable();
        }
        return true;
    }

    @Override
    public void remove(final String name) {
        this.faultItemTable.remove(name);
    }

    //ok  至少选出一个broker名
    @Override
    public String pickOneAtLeast() {
        //拿出列表中所有错误项放入临时列表tmpList
        final Enumeration<FaultItem> elements = this.faultItemTable.elements();
        List<FaultItem> tmpList = new LinkedList<FaultItem>();
        while (elements.hasMoreElements()) {
            final FaultItem faultItem = elements.nextElement();
            tmpList.add(faultItem);
        }
        if (!tmpList.isEmpty()) {  //假设tmpList有100个
            //如果列表不为空，就打乱再排序
            Collections.shuffle(tmpList);
            Collections.sort(tmpList); //排序，越后面的越糟糕
            final int half = tmpList.size() / 2;  //half = 50
            if (half <= 0) {
                return tmpList.get(0).getName();
            } else {
                //whichItemWorst第一次生成一个随机数，后面每次+1，对50取余，最多就是49
                final int i = this.whichItemWorst.incrementAndGet() % half;
                //也就是返回排序后前面一半故障项中的一个，为啥就是最糟糕呢？
                return tmpList.get(i).getName();
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "LatencyFaultToleranceImpl{" +
            "faultItemTable=" + faultItemTable +
            ", whichItemWorst=" + whichItemWorst +
            '}';
    }

    //ok  内部类，故障项，实现可排序接口，当排序时，外界不指定排序逻辑时就按照本类定义的逻辑排序
    class FaultItem implements Comparable<FaultItem> {
        private final String name;  //broker名
        private volatile long currentLatency;  //当前延迟
        private volatile long startTimestamp;  //开始时间戳，代表这个故障项能开始的时间

        public FaultItem(final String name) {
            this.name = name;
        }

        //return 1代表排序放在后面 根据排序逻辑可知，排序靠后的越差
        @Override
        public int compareTo(final FaultItem other) {
            //如果两个故障项可用状态不同，把isAvailable是false的放后面，也就是不可用的放后面
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable())
                    return -1;
                if (other.isAvailable())
                    return 1;
            }
            //可用状态相同，当前延迟大的放后面
            if (this.currentLatency < other.currentLatency)
                return -1;
            else if (this.currentLatency > other.currentLatency) {
                return 1;
            }
            //否则，开始时间戳大的放在后面
            if (this.startTimestamp < other.startTimestamp)
                return -1;
            else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }
            return 0;
        }

        //是否可用 如果当前时间大于故障项开始时间就是可用
        public boolean isAvailable() {
            return (System.currentTimeMillis() - startTimestamp) >= 0;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof FaultItem))
                return false;

            final FaultItem faultItem = (FaultItem) o;

            if (getCurrentLatency() != faultItem.getCurrentLatency())
                return false;
            if (getStartTimestamp() != faultItem.getStartTimestamp())
                return false;
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;

        }

        @Override
        public String toString() {
            return "FaultItem{" +
                "name='" + name + '\'' +
                ", currentLatency=" + currentLatency +
                ", startTimestamp=" + startTimestamp +
                '}';
        }

        public String getName() {
            return name;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(final long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(final long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

    }
}
