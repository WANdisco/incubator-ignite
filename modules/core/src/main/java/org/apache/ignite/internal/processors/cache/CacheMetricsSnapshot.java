/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * Metrics snapshot.
 */
class CacheMetricsSnapshot implements CacheMetrics {
    /** Number of reads. */
    private long reads = 0;

    /** Number of puts. */
    private long puts = 0;

    /** Number of hits. */
    private long hits = 0;

    /** Number of misses. */
    private long misses = 0;

    /** Number of transaction commits. */
    private long txCommits = 0;

    /** Number of transaction rollbacks. */
    private long txRollbacks = 0;

    /** Number of evictions. */
    private long evicts = 0;

    /** Number of removed entries. */
    private long removes = 0;

    /** Put time taken nanos. */
    private float putAvgTimeNanos = 0;

    /** Get time taken nanos. */
    private float getAvgTimeNanos = 0;

    /** Remove time taken nanos. */
    private float removeAvgTimeNanos = 0;

    /** Commit transaction time taken nanos. */
    private float commitAvgTimeNanos = 0;

    /** Commit transaction time taken nanos. */
    private float rollbackAvgTimeNanos = 0;

    /** Cache name */
    private String cacheName;

    /** Number of entries that was swapped to disk. */
    private long overflowSize;

    /** Number of entries stored in off-heap memory. */
    private long offHeapEntriesCount;

    /** Memory size allocated in off-heap. */
    private long offHeapAllocatedSize;

    /** Number of non-{@code null} values in the cache. */
    private int size;

    /** Number of keys in the cache, possibly with {@code null} values. */
    private int keySize;

    /** Cache is empty. */
    private boolean isEmpty;

    /** Gets current size of evict queue used to batch up evictions. */
    private int dhtEvictQueueCurrentSize;

    /** Transaction per-thread map size. */
    private int txThreadMapSize;

    /** Transaction per-Xid map size. */
    private int txXidMapSize;

    /** Committed transaction queue size. */
    private int txCommitQueueSize;

    /** Prepared transaction queue size. */
    private int txPrepareQueueSize;

    /** Start version counts map size. */
    private int txStartVersionCountsSize;

    /** Number of cached committed transaction IDs. */
    private int txCommittedVersionsSize;

    /** Number of cached rolled back transaction IDs. */
    private int txRolledbackVersionsSize;

    /** DHT thread map size. */
    private int txDhtThreadMapSize;

    /** Transaction DHT per-Xid map size. */
    private int txDhtXidMapSize;

    /** Committed DHT transaction queue size. */
    private int txDhtCommitQueueSize;

    /** Prepared DHT transaction queue size. */
    private int txDhtPrepareQueueSize;

    /** DHT start version counts map size. */
    private int txDhtStartVersionCountsSize;

    /** Number of cached committed DHT transaction IDs. */
    private int txDhtCommittedVersionsSize;

    /** Number of cached rolled back DHT transaction IDs. */
    private int txDhtRolledbackVersionsSize;

    /** Write-behind is enabled. */
    private boolean isWriteBehindEnabled;

    /** Buffer size that triggers flush procedure. */
    private int writeBehindFlushSize;

    /** Count of worker threads. */
    private int writeBehindFlushThreadCount;

    /** Flush frequency in milliseconds. */
    private long writeBehindFlushFrequency;

    /** Maximum size of batch. */
    private int writeBehindStoreBatchSize;

    /** Count of cache overflow events since start. */
    private int writeBehindTotalCriticalOverflowCount;

    /** Count of cache overflow events since start. */
    private int writeBehindCriticalOverflowCount;

    /** Count of entries in store-retry state. */
    private int writeBehindErrorRetryCount;

    /** Total count of entries in cache store internal buffer. */
    private int writeBehindBufferSize;

    /** */
    private String keyType;

    /** */
    private String valueType;

    /** */
    private boolean isStoreByValue;

    /** */
    private boolean isStatisticsEnabled;

    /** */
    private boolean isManagementEnabled;

    /** */
    private boolean isReadThrough;

    /** */
    private boolean isWriteThrough;

    /**
     * Create snapshot for given metrics.
     *
     * @param m Cache metrics.
     */
    public CacheMetricsSnapshot(CacheMetrics m) {
        reads = m.getCacheGets();
        puts = m.getCachePuts();
        hits = m.getCacheHits();
        misses = m.getCacheMisses();
        txCommits = m.getCacheTxCommits();
        txRollbacks = m.getCacheTxRollbacks();
        evicts = m.getCacheEvictions();
        removes = m.getCacheRemovals();

        putAvgTimeNanos = m.getAveragePutTime();
        getAvgTimeNanos = m.getAverageGetTime();
        removeAvgTimeNanos = m.getAverageRemoveTime();
        commitAvgTimeNanos = m.getAverageTxCommitTime();
        rollbackAvgTimeNanos = m.getAverageTxRollbackTime();

        cacheName = m.name();
        overflowSize = m.getOverflowSize();
        offHeapEntriesCount = m.getOffHeapEntriesCount();
        offHeapAllocatedSize = m.getOffHeapAllocatedSize();
        size = m.getSize();
        keySize = m.getKeySize();
        isEmpty = m.isEmpty();
        dhtEvictQueueCurrentSize = m.getDhtEvictQueueCurrentSize();
        txThreadMapSize = m.getTxThreadMapSize();
        txXidMapSize = m.getTxXidMapSize();
        txCommitQueueSize = m.getTxCommitQueueSize();
        txPrepareQueueSize = m.getTxPrepareQueueSize();
        txStartVersionCountsSize = m.getTxStartVersionCountsSize();
        txCommittedVersionsSize = m.getTxCommittedVersionsSize();
        txRolledbackVersionsSize = m.getTxRolledbackVersionsSize();
        txDhtThreadMapSize = m.getTxDhtThreadMapSize();
        txDhtXidMapSize = m.getTxDhtXidMapSize();
        txDhtCommitQueueSize = m.getTxDhtCommitQueueSize();
        txDhtPrepareQueueSize = m.getTxDhtPrepareQueueSize();
        txDhtStartVersionCountsSize = m.getTxDhtStartVersionCountsSize();
        txDhtCommittedVersionsSize = m.getTxDhtCommittedVersionsSize();
        txDhtRolledbackVersionsSize = m.getTxDhtRolledbackVersionsSize();
        isWriteBehindEnabled = m.isWriteBehindEnabled();
        writeBehindFlushSize = m.getWriteBehindFlushSize();
        writeBehindFlushThreadCount = m.getWriteBehindFlushThreadCount();
        writeBehindFlushFrequency = m.getWriteBehindFlushFrequency();
        writeBehindStoreBatchSize = m.getWriteBehindStoreBatchSize();
        writeBehindTotalCriticalOverflowCount = m.getWriteBehindTotalCriticalOverflowCount();
        writeBehindCriticalOverflowCount = m.getWriteBehindCriticalOverflowCount();
        writeBehindErrorRetryCount = m.getWriteBehindErrorRetryCount();
        writeBehindBufferSize = m.getWriteBehindBufferSize();

        keyType = m.getKeyType();
        valueType = m.getValueType();
        isStoreByValue = m.isStoreByValue();
        isStatisticsEnabled = m.isStatisticsEnabled();
        isManagementEnabled = m.isManagementEnabled();
        isReadThrough = m.isReadThrough();
        isWriteThrough = m.isWriteThrough();
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return hits;
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        if (hits == 0 || reads == 0)
            return 0;

        return (float) hits / reads * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return misses;
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        if (misses == 0 || reads == 0) {
            return 0;
        }

        return (float) misses / reads * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return reads;
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return puts;
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return removes;
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return evicts;
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        return getAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        return putAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        return removeAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxCommitTime() {
        return commitAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxRollbackTime() {
        return rollbackAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return txCommits;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return txRollbacks;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public long getOverflowSize() {
        return overflowSize;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEntriesCount() {
        return offHeapEntriesCount;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapAllocatedSize() {
        return offHeapAllocatedSize;
    }

    /** {@inheritDoc} */
    @Override public int getSize() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public int getKeySize() {
        return keySize;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return isEmpty;
    }

    /** {@inheritDoc} */
    @Override public int getDhtEvictQueueCurrentSize() {
        return dhtEvictQueueCurrentSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxThreadMapSize() {
        return txThreadMapSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxXidMapSize() {
        return txXidMapSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxCommitQueueSize() {
        return txCommitQueueSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxPrepareQueueSize() {
        return txPrepareQueueSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxStartVersionCountsSize() {
        return txStartVersionCountsSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxCommittedVersionsSize() {
        return txCommittedVersionsSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxRolledbackVersionsSize() {
        return txRolledbackVersionsSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtThreadMapSize() {
        return txDhtThreadMapSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtXidMapSize() {
        return txDhtXidMapSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommitQueueSize() {
        return txDhtCommitQueueSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtPrepareQueueSize() {
        return txDhtPrepareQueueSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtStartVersionCountsSize() {
        return txDhtStartVersionCountsSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommittedVersionsSize() {
        return txDhtCommittedVersionsSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtRolledbackVersionsSize() {
        return txDhtRolledbackVersionsSize;
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteBehindEnabled() {
        return isWriteBehindEnabled;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushSize() {
        return writeBehindFlushSize;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushThreadCount() {
        return writeBehindFlushThreadCount;
    }

    /** {@inheritDoc} */
    @Override public long getWriteBehindFlushFrequency() {
        return writeBehindFlushFrequency;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindStoreBatchSize() {
        return writeBehindStoreBatchSize;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindTotalCriticalOverflowCount() {
        return writeBehindTotalCriticalOverflowCount;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindCriticalOverflowCount() {
        return writeBehindCriticalOverflowCount;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindErrorRetryCount() {
        return writeBehindErrorRetryCount;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindBufferSize() {
        return writeBehindBufferSize;
    }

    /** {@inheritDoc} */
    @Override public String getKeyType() {
        return keyType;
    }

    /** {@inheritDoc} */
    @Override public String getValueType() {
        return valueType;
    }

    /** {@inheritDoc} */
    @Override public boolean isStoreByValue() {
        return isStoreByValue;
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return isStatisticsEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        return isManagementEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean isReadThrough() {
        return isReadThrough;
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        return isWriteThrough;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheMetricsSnapshot.class, this);
    }
}
