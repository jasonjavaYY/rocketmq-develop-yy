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

package org.apache.rocketmq.filter.util;

import com.google.common.hash.Hashing;

import java.nio.charset.Charset;

/**
 * Simple implement of bloom filter.
 */
//ok  布隆过滤器简化实现
public class BloomFilter {
    //布隆过滤器就是一个很长的向量，每个位置存放0或者1，通过多种方式计算某个对象的hash值，将对应位置置为1，
    // 则后面用这个过滤器去过滤对象，如果能匹配到对应位置都是1的某个对象，也不一定能保证一定存在这个对象，
    // 因为可能两个不同的对象经过多种方式计算得到完全相同的hash值，小概率事件。但是如果匹配不到，一定说明不存在该对象，
    // 所以布隆过滤器用来判断是否一定不存在，没法判断是否存在。因为某个向量可能对应到多个对象，因此布隆过滤器也做不到根据向量精准删除对象
    // 向量长度太短，错误率很高，向量长度太长浪费空间
    public static final Charset UTF_8 = Charset.forName("UTF-8");

    // as error rate, 10/100 = 0.1
    private int f = 10;
    private int n = 128;

    // hash function num, by calculation.
    private int k;
    // bit count, by calculation.
    private int m;

    /**
     * Create bloom filter by error rate and mapping num.
     *
     * @param f error rate
     * @param n num will mapping to bit
     */
    //通过f和n创建一个布隆过滤器，f/100是错误率，n是映射的比特位
    public static BloomFilter createByFn(int f, int n) {
        return new BloomFilter(f, n);
    }

    /**
     * Constructor.
     *
     * @param f error rate
     * @param n num will mapping to bit
     */
    private BloomFilter(int f, int n) {
        //f的合理值判断
        if (f < 1 || f >= 100) {
            throw new IllegalArgumentException("f must be greater or equal than 1 and less than 100");
        }
        //n必须大于0
        if (n < 1) {
            throw new IllegalArgumentException("n must be greater than 0");
        }
        //设置f和n
        this.f = f;
        this.n = n;
        //根据f和n计算k和m
        // set p = e^(-kn/m)
        // f = (1 - p)^k = e^(kln(1-p))   好数学~
        // when p = 0.5, k = ln2 * (m/n), f = (1/2)^k = (0.618)^(m/n)
        double errorRate = f / 100.0;
        //计算0.5为底错误率的对数向下取整作为k
        this.k = (int) Math.ceil(logMN(0.5, errorRate));
        //如果计算出k小于1，抛异常
        if (this.k < 1) {
            throw new IllegalArgumentException("Hash function num is less than 1, maybe you should change the value of error rate or bit num!");
        }

        // m >= n*log2(1/f)*log2(e)
        this.m = (int) Math.ceil(this.n * logMN(2, 1 / errorRate) * logMN(2, Math.E));
        // m%8 = 0
        this.m = (int) (Byte.SIZE * Math.ceil(this.m / (Byte.SIZE * 1.0)));
    }

    /**
     * Calculate bit positions of {@code str}.
     * <p>
     * See "Less Hashing, Same Performance: Building a Better Bloom Filter" by Adam Kirsch and Michael
     * Mitzenmacher.
     * </p>
     */
    //根据字符串计算比特位
    public int[] calcBitPositions(String str) {
        int[] bitPositions = new int[this.k];
        //计算出字符串的哈希64
        long hash64 = Hashing.murmur3_128().hashString(str, UTF_8).asLong();
        //得到本身哈希和右移32位的哈希
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= this.k; i++) {
            //结合两个哈希值，并且给第二个哈希*i，为了更分散
            int combinedHash = hash1 + (i * hash2);
            // Flip all the bits if it's negative (guaranteed positive number)
            //如果计算出的结合哈希为负，按位取反
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            //哈希值对m取余放入比特位
            bitPositions[i - 1] = combinedHash % this.m;
        }
        return bitPositions;
    }

    /**
     * Calculate bit positions of {@code str} to construct {@code BloomFilterData}
     */
    //根据字符串生成一个布隆过滤器数据
    public BloomFilterData generate(String str) {
        int[] bitPositions = calcBitPositions(str);
        return new BloomFilterData(bitPositions, this.m);
    }

    /**
     * Calculate bit positions of {@code str}, then set the related {@code bits} positions to 1.
     */
    public void hashTo(String str, BitsArray bits) {
        hashTo(calcBitPositions(str), bits);
    }

    /**
     * Set the related {@code bits} positions to 1.
     */
    public void hashTo(int[] bitPositions, BitsArray bits) {
        check(bits);

        for (int i : bitPositions) {
            bits.setBit(i, true);
        }
    }

    /**
     * Extra check:
     * <li>1. check {@code filterData} belong to this bloom filter.</li>
     * <p>
     * Then set the related {@code bits} positions to 1.
     * </p>
     */
    public void hashTo(BloomFilterData filterData, BitsArray bits) {
        if (!isValid(filterData)) {
            throw new IllegalArgumentException(
                String.format("Bloom filter data may not belong to this filter! %s, %s",
                    filterData, this.toString())
            );
        }
        hashTo(filterData.getBitPos(), bits);
    }

    /**
     * Calculate bit positions of {@code str}, then check all the related {@code bits} positions is 1.
     *
     * @return true: all the related {@code bits} positions is 1
     */
    //判断某个字符串是否命中
    public boolean isHit(String str, BitsArray bits) {
        return isHit(calcBitPositions(str), bits);
    }

    /**
     * Check all the related {@code bits} positions is 1.
     *
     * @return true: all the related {@code bits} positions is 1
     */
    public boolean isHit(int[] bitPositions, BitsArray bits) {
        check(bits);
        boolean ret = bits.getBit(bitPositions[0]);
        for (int i = 1; i < bitPositions.length; i++) {
            ret &= bits.getBit(bitPositions[i]);
        }
        return ret;
    }

    /**
     * Check all the related {@code bits} positions is 1.
     *
     * @return true: all the related {@code bits} positions is 1
     */
    public boolean isHit(BloomFilterData filterData, BitsArray bits) {
        if (!isValid(filterData)) {
            throw new IllegalArgumentException(
                String.format("Bloom filter data may not belong to this filter! %s, %s",
                    filterData, this.toString())
            );
        }
        return isHit(filterData.getBitPos(), bits);
    }

    /**
     * Check whether one of {@code bitPositions} has been occupied.
     *
     * @return true: if all positions have been occupied.
     */
    //检查错误命中，如果所有位置都被占用就返回true
    public boolean checkFalseHit(int[] bitPositions, BitsArray bits) {
        for (int j = 0; j < bitPositions.length; j++) {
            int pos = bitPositions[j];

            // check position of bits has been set.
            // that mean no one occupy the position.
            if (!bits.getBit(pos)) {
                return false;
            }
        }

        return true;
    }

    //ok  检查bit数组长度是否为m
    protected void check(BitsArray bits) {
        if (bits.bitLength() != this.m) {
            throw new IllegalArgumentException(
                String.format("Length(%d) of bits in BitsArray is not equal to %d!", bits.bitLength(), this.m)
            );
        }
    }

    /**
     * Check {@code BloomFilterData} is valid, and belong to this bloom filter.
     * <li>1. not null</li>
     * <li>2. {@link org.apache.rocketmq.filter.util.BloomFilterData#getBitNum} must be equal to {@code m} </li>
     * <li>3. {@link org.apache.rocketmq.filter.util.BloomFilterData#getBitPos} is not null</li>
     * <li>4. {@link org.apache.rocketmq.filter.util.BloomFilterData#getBitPos}'s length is equal to {@code k}</li>
     */
    //检查某个布隆过滤器数据是否有效
    public boolean isValid(BloomFilterData filterData) {
        if (filterData == null
            || filterData.getBitNum() != this.m
            || filterData.getBitPos() == null
            || filterData.getBitPos().length != this.k) {
            return false;
        }
        return true;
    }

    /**
     * error rate.
     */
    public int getF() {
        return f;
    }

    /**
     * expect mapping num.
     */
    public int getN() {
        return n;
    }

    /**
     * hash function num.
     */
    public int getK() {
        return k;
    }

    /**
     * total bit num.
     */
    public int getM() {
        return m;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof BloomFilter))
            return false;

        BloomFilter that = (BloomFilter) o;

        if (f != that.f)
            return false;
        if (k != that.k)
            return false;
        if (m != that.m)
            return false;
        if (n != that.n)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = f;
        result = 31 * result + n;
        result = 31 * result + k;
        result = 31 * result + m;
        return result;
    }

    @Override
    public String toString() {
        return String.format("f: %d, n: %d, k: %d, m: %d", f, n, k, m);
    }

    //log以m为底n的对数
    protected double logMN(double m, double n) {
        return Math.log(n) / Math.log(m);
    }
}
