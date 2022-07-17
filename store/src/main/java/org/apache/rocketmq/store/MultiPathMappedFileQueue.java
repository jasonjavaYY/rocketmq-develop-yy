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


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
//ok  多路径mf文件队列
public class MultiPathMappedFileQueue extends MappedFileQueue {
    private final MessageStoreConfig config;   //消息存储配置
    private final Supplier<Set<String>> fullStorePathsSupplier;   //全部存储路径

    public MultiPathMappedFileQueue(MessageStoreConfig messageStoreConfig, int mappedFileSize,
                                    AllocateMappedFileService allocateMappedFileService,
                                    Supplier<Set<String>> fullStorePathsSupplier) {
        super(messageStoreConfig.getStorePathCommitLog(), mappedFileSize, allocateMappedFileService);
        this.config = messageStoreConfig;
        this.fullStorePathsSupplier = fullStorePathsSupplier;
    }
    //获取所有cl的存储路径
    private Set<String> getPaths() {
        String[] paths = config.getStorePathCommitLog().trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
        return new HashSet<>(Arrays.asList(paths));
    }
    //获取所有只读路径
    private Set<String> getReadonlyPaths() {
        String pathStr = config.getReadOnlyCommitLogStorePaths();
        if (StringUtils.isBlank(pathStr)) {
            return Collections.emptySet();
        }
        String[] paths = pathStr.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
        return new HashSet<>(Arrays.asList(paths));
    }

    //ok  加载
    @Override
    public boolean load() {
        //获取所有cl存储路径，再加入所有只读路径，获取这些路径下的所有文件，加入文件列表
        Set<String> storePathSet = getPaths();
        storePathSet.addAll(getReadonlyPaths());
        List<File> files = new ArrayList<>();
        for (String path : storePathSet) {
            File dir = new File(path);
            File[] ls = dir.listFiles();
            if (ls != null) {
                Collections.addAll(files, ls);
            }
        }
        //加载所有文件
        return doLoad(files);
    }

    //ok  尝试创建mf
    @Override
    protected MappedFile tryCreateMappedFile(long createOffset) {
        //计算文件下标
        long fileIdx = createOffset / this.mappedFileSize;
        //获取所有cl路径和只读路径
        Set<String> storePath = getPaths();
        Set<String> readonlyPathSet = getReadonlyPaths();
        //获取全部存储路径
        Set<String> fullStorePaths =
                fullStorePathsSupplier == null ? Collections.emptySet() : fullStorePathsSupplier.get();
        //建议可用存储路径，清除只读路径和提供的路径
        HashSet<String> availableStorePath = new HashSet<>(storePath);
        //do not create file in readonly store path.
        availableStorePath.removeAll(readonlyPathSet);
        //do not create file is space is nearly full.
        availableStorePath.removeAll(fullStorePaths);

        //if no store path left, fall back to writable store path.
        if (availableStorePath.isEmpty()) {
            availableStorePath = new HashSet<>(storePath);
            availableStorePath.removeAll(readonlyPathSet);
        }
        //创建mf文件
        String[] paths = availableStorePath.toArray(new String[]{});
        Arrays.sort(paths);
        String nextFilePath = paths[(int) (fileIdx % paths.length)] + File.separator
                + UtilAll.offset2FileName(createOffset);
        String nextNextFilePath = paths[(int) ((fileIdx + 1) % paths.length)] + File.separator
                + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    //ok  销毁
    @Override
    public void destroy() {
        //遍历所有mf，执行销毁方法
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        //清除mf列表
        this.mappedFiles.clear();
        this.flushedWhere = 0;
        //获取所有存储路径和只读路径，删除路径
        Set<String> storePathSet = getPaths();
        storePathSet.addAll(getReadonlyPaths());
        for (String path : storePathSet) {
            File file = new File(path);
            if (file.isDirectory()) {
                file.delete();
            }
        }
    }
}
