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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
//ok  mf队列
public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final int DELETE_FILES_BATCH_MAX = 10;
    private final String storePath;
    protected final int mappedFileSize;
    protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();
    private final AllocateMappedFileService allocateMappedFileService;
    protected long flushedWhere = 0;
    private long committedWhere = 0;
    private volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    //ok  自检
    public void checkSelf() {
        //如果MF列表不为空
        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                //遍历MF列表，每个MF都减去前一个MF的from偏移量，判断是否等于MF大小，如果不相等，说明这个MF受到损坏
                MappedFile cur = iterator.next();
                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    //根据时间戳获取MF方法
    public MappedFile getMappedFileByTime(final long timestamp) {
        //先复制MF文件
        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs)
            return null;
        for (int i = 0; i < mfs.length; i++) {
            //遍历复制的MF列表，如果MF的上一次修改时间戳超过指定时间戳，就返回
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }
        //上面没找到，就返回最后一个MF，因为MF是按时间顺序写入的
        return (MappedFile) mfs[mfs.length - 1];
    }

    //复制MF
    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        //如果当前MF文件大小小于指定预留大小，返回空，否则返回MF文件数组
        Object[] mfs;
        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }
        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    //按偏移量截断脏文件
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
        for (MappedFile file : this.mappedFiles) {
            //遍历MF列表，获取文件的结束偏移量，如果结束偏移量超过指定的偏移量
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                //如果指定偏移量不小于文件from偏移，就修改文件末尾位置到指定偏移量
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    //超出的那些MF文件直接移除掉
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }
        this.deleteExpiredFile(willRemoveFiles);
    }

    //删除失效文件
    void deleteExpiredFile(List<MappedFile> files) {
        if (!files.isEmpty()) {
            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                //遍历要删除的文件，去除MF列表中不含的文件
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }
            //从MF列表中去除上面过滤出来的包含的文件
            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    //ok 加载
    public boolean load() {
        //根据路径创建文件，列出所有的文件，针对这些文件执行创建
        File dir = new File(this.storePath);
        File[] ls = dir.listFiles();
        if (ls != null) {
            return doLoad(Arrays.asList(ls));
        }
        return true;
    }

    //ok  真实加载
    public boolean doLoad(List<File> files) {
        // ascending order  根据文件名排序
        files.sort(Comparator.comparing(File::getName));
        for (File file : files) {
            //遍历文件，如果文件长度和MF大小不等，报错返回false
            if (file.length() != this.mappedFileSize) {
                log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                return false;
            }
            //根据文件路径和MF大小创建MF，更新MF的写位置、刷新位置、提交位置，将MF加入MF列表
            try {
                MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                mappedFile.setWrotePosition(this.mappedFileSize);
                mappedFile.setFlushedPosition(this.mappedFileSize);
                mappedFile.setCommittedPosition(this.mappedFileSize);
                this.mappedFiles.add(mappedFile);
                log.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                log.error("load file " + file + " error", e);
                return false;
            }
        }
        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    //ok  获取最后一个MF
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        //获取最后一个MF
        MappedFile mappedFileLast = getLastMappedFile();
        //如果最后一个MF为空，就更新创建偏移量，假设startOffset=90，mappedFileSize=40，
        // 因为MF是连续的，所以虽然指定了90，但也应该从位置80开始创建，所以是90-（90%40）=80
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }
        //如果最后一个MF不为空并且它满了，创建位置就是最后一个MF的from偏移+MF大小
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }
        //如果有创建偏移量并且需要创建，就尝试创建一个新的MF成为最后一个MF
        if (createOffset != -1 && needCreate) {
            return tryCreateMappedFile(createOffset);
        }
        //返回之前的最后一个MF
        return mappedFileLast;
    }

    //ok  尝试创建MF，构造next路径和nextNext路径，创建MF
    protected MappedFile tryCreateMappedFile(long createOffset) {
        String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
        String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset
                + this.mappedFileSize);
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    //Ok  创建MF
    protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
        MappedFile mappedFile = null;
        if (this.allocateMappedFileService != null) {
            //如果有分配MF线程类，就根据next路径和nextNext路径创建执行分配请求并返回MF
            mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
        } else {
            try {
                //没有分配MF服务，就用构造方法创建一个MF
                mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
            } catch (IOException e) {
                log.error("create mappedFile exception", e);
            }
        }
        //将创建的MF加入队列中，返回MF
        if (mappedFile != null) {
            if (this.mappedFiles.isEmpty()) {
                mappedFile.setFirstCreateInQueue(true);
            }
            this.mappedFiles.add(mappedFile);
        }
        return mappedFile;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    //ok  获取最近的MF 就是返回MF列表最后一个MF
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;
        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }
        return mappedFileLast;
    }

    //ok  重设偏移量
    public boolean resetOffset(long offset) {
        //获取最后一个MF
        MappedFile mappedFileLast = getLastMappedFile();
        if (mappedFileLast != null) {
            //如果最后一个MF不为空，获取当前最后的偏移量，计算和入参的差值，如果差值超过2个MF大小，快速失败返回false
            long lastOffset = mappedFileLast.getFileFromOffset() +
                mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;
            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }
        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();
        while (iterator.hasPrevious()) {
            //从后往前遍历MF列表，找到入参偏移量超过from偏移的第一个MF，更后面的MF都直接去除
            //将这个MF的刷新位置、写位置、提交位置都设置为指定的偏移量
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    //ok  根据时间删除失效文件
    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately) {
        //复制MF列表
        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs)
            return 0;
        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                //遍历复制的MF列表
                MappedFile mappedFile = (MappedFile) mfs[i];
                //计算MF的最大存活时间，如果当前时间超过了存活时间或者传入了立即删除，就销毁MF，将这个MF放入待删除列表
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;
                        //如果要删除的MF列表超过了删除批次文件数上限，直接退出
                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }
        //执行删除文件，返回删除文件的个数
        deleteExpiredFile(files);
        return deleteCount;
    }

    //ok  根据偏移量删除失效文件
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        //复制MF列表
        Object[] mfs = this.copyMappedFiles(0);
        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {
            int mfsLength = mfs.length - 1;
            for (int i = 0; i < mfsLength; i++) {
                //遍历复制的MF列表
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                //从MF大小-单位大小位置获取buffer
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    //获取buffer的偏移量，如果偏移量小于入参偏移量，就销毁
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }
                //如果MF销毁成功，将MF加入待删除的文件列表，删除该文件
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }
        deleteExpiredFile(files);   //删除失效文件
        return deleteCount;  //返回删除文件数
    }

    //ok  刷新
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        //找到flush位置的MF，如果flush位置是0就说明是第一次flush，就返回第一个MF
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            //如果找到了MF，执行MF的刷新，更新flush位置
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }
        return result;
    }

    //ok  提交
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        //找到提交位置的MF
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            //如果MF不为空，执行提交，更新提交位置
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere;
            this.committedWhere = where;
        }
        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */   //ok 根据偏移量获取MF
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile();  //ok  获取第一个MF和最后一个MF
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                //如果第一个和最后一个MF都不为空，判断偏移量是否合法，不能小于第一个MF的开始偏移，不能超过最后一个MF的最大偏移
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
                } else {
                    //计算目标MF的index，指定偏移量/MF大小-第一个MF的开始偏移/MF大小就是要删除的MF的index，如果所有MF都是连续等大没有丢失，这里的index位置MF就是要找的MF
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);  //获取目标MF
                    } catch (Exception ignored) {
                    }
                    //判断如果目标MF不为空且指定偏移超过目标MF的起始偏移且指定偏移量小于目标MF的最大偏移，就返回该MF
                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                        && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }
                    //又遍历MF列表找了一次，因为MF列表中可能出现MF不连续或者丢失了某个MF的情况，上面计算出的index就不准确了，
                    // 这里再找一次，如果能找到满足偏移量的MF，就返回
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                            && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }
                //上面的遍历如果还没找到，就是真的没有匹配的MF，如果指定了找不到返回第一个，就返回第一个MF
                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }
        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }
        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    //ok  销毁
    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            //遍历每个MF执行销毁
            mf.destroy(1000 * 3);
        }
        //清除MF文件列表
        this.mappedFiles.clear();
        this.flushedWhere = 0;
        // delete parent directory 删除父目录
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
