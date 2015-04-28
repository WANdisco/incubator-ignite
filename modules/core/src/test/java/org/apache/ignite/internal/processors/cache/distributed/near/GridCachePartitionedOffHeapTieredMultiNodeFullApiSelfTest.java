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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;

import static org.apache.ignite.cache.CacheMemoryMode.*;

/**
 * Tests partitioned cache with off-heap tiered mode.
 */
public class GridCachePartitionedOffHeapTieredMultiNodeFullApiSelfTest extends GridCachePartitionedFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} *//*
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }*/

    /** {@inheritDoc} */
    @Override protected CacheMemoryMode memoryMode() {
        return OFFHEAP_TIERED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        IgniteCache cache = grid(0).cache(null);

        for (int i = 4; i < 5; ++i) {
            String key = "key_" + 1;
            System.out.println("key = " + key);

            cache.put(key, i);

            for (int j = 0; j < gridCount(); ++j) {
                boolean isPrimary = grid(0).affinity(null).isPrimary(grid(j).localNode(), key);

                boolean isBackup = grid(0).affinity(null).isBackup(grid(j).localNode(), key);

                System.out.println("Grid " + j + ", primary=" + isPrimary + ", backup=" + isBackup);

                if (isPrimary) {
                    assertNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.ONHEAP));
                    assertNotNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.OFFHEAP));
                    assertNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.SWAP));
                    assertNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.NEAR));
                    assertNotNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.PRIMARY));
                    assertNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.BACKUP));
                }

                if (isBackup) {
                    assertNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.ONHEAP));
                    assertNotNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.OFFHEAP));
                    assertNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.SWAP));
                    assertNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.NEAR));
                    assertNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.PRIMARY));
                    assertNotNull(grid(j).cache(null).withSkipStore().localPeek(key, CachePeekMode.BACKUP));
                }
            }
        }
    }
}
