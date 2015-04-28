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

import static org.apache.ignite.cache.CacheMemoryMode.*;

/**
 * Tests partitioned cache with off-heap tiered mode.
 */
public class GridCachePartitionedOffHeapTieredMultiNodeFullApiSelfTest extends GridCachePartitionedFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMemoryMode memoryMode() {
        return OFFHEAP_TIERED;
    }

    /**
    * @throws Exception If failed.
    */
    public void testPutRemove() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).cache(null);

        String key = "key_" + 4;

        map.put(key, 4);

        IgniteCache<String, Integer> primaryCache = primaryCache(key);

        assertFalse(grid(0).affinity(null).isPrimary(grid(0).localNode(), key));
        assertFalse(grid(0).affinity(null).isBackup(grid(0).localNode(), key));

        assertEquals(4, cache.get(key).intValue());
        assertNull(primaryCache.localPeek(key, CachePeekMode.ONHEAP));
        assertEquals(4, primaryCache.localPeek(key, CachePeekMode.OFFHEAP).intValue());

        cache.put(key, 5);

        assertEquals(5, primaryCache.localPeek(key, CachePeekMode.ONHEAP).intValue());
        assertEquals(5, primaryCache.localPeek(key, CachePeekMode.OFFHEAP).intValue());
        assertEquals(5, cache.get(key).intValue());
        assertEquals(4, map.get(key));

        /*

        cache.put(key, 4);

        for (int i = 0; i < gridCount(); ++i) {
            System.out.println("After put : Grid " + i +
                " is primary " + grid(0).affinity(null).isPrimary(grid(i).localNode(), key) +
                " is backup " + grid(0).affinity(null).isBackup(grid(i).localNode(), key));
            System.out.println("After put : On heap "  + grid(i).cache(null).withSkipStore().localPeek(key, ONHEAP_PEEK_MODES));
            System.out.println("After put : Off heap "  + grid(i).cache(null).withSkipStore().localPeek(key, CachePeekMode.OFFHEAP));
        }

        assertEquals(4, cacheSkipStore.get(key).intValue());
        assertEquals(4, cache.get(key).intValue());
        assertEquals(4, map.get(key));

        for (int i = 0; i < gridCount(); ++i) {
            System.out.println("After put + get: Grid " + i +
                " is primary " + grid(0).affinity(null).isPrimary(grid(i).localNode(), key) +
                " is backup " + grid(0).affinity(null).isBackup(grid(i).localNode(), key));
            System.out.println("After put + get: On heap "  + grid(i).cache(null).localPeek(key, ONHEAP_PEEK_MODES));
            System.out.println("After put + get: Off heap "  + grid(i).cache(null).localPeek(key, CachePeekMode.OFFHEAP));
        }
        cacheSkipStore.remove(key);

        for (int i = 0; i < gridCount(); ++i) {
            System.out.println("After put + get + remove: Grid " + i +
                " is primary " + grid(0).affinity(null).isPrimary(grid(i).localNode(), key) +
                " is backup " + grid(0).affinity(null).isBackup(grid(i).localNode(), key));
            System.out.println("After put + get + remove: On heap "  + grid(i).cache(null).withSkipStore().localPeek(key, ONHEAP_PEEK_MODES));
            System.out.println("After put + get + remove: Off heap "  + grid(i).cache(null).withSkipStore().localPeek(key, CachePeekMode.OFFHEAP));
        }

        assertNull(cacheSkipStore.get(key));
        assertEquals(4, cache.get(key).intValue());
        assertEquals(4, map.get(key));

        for (int i = 0; i < gridCount(); ++i) {
            System.out.println("After put + get + remove + get: Grid " + i +
                " is primary " + grid(0).affinity(null).isPrimary(grid(i).localNode(), key) +
                " is backup " + grid(0).affinity(null).isBackup(grid(i).localNode(), key));
            System.out.println("After put + get + remove + get: On heap "  + grid(i).cache(null).withSkipStore().localPeek(key, ONHEAP_PEEK_MODES));
            System.out.println("After put + get + remove + get: Off heap "  + grid(i).cache(null).withSkipStore().localPeek(key, CachePeekMode.OFFHEAP));
        }

        cacheSkipStore.put(key, 5);
        for (int i = 0; i < gridCount(); ++i) {
            System.out.println("After put + get + remove + get + put: Grid " + i +
                " is primary " + grid(0).affinity(null).isPrimary(grid(i).localNode(), key) +
                " is backup " + grid(0).affinity(null).isBackup(grid(i).localNode(), key));
            System.out.println("After put + get + remove + get + put: On heap "  + grid(i).cache(null).withSkipStore().localPeek(key, ONHEAP_PEEK_MODES));
            System.out.println("After put + get + remove + get + put: Off heap "  + grid(i).cache(null).withSkipStore().localPeek(key, CachePeekMode.OFFHEAP));
        }

        assertEquals(5, cacheSkipStore.get(key).intValue());
        assertEquals(5, cache.get(key).intValue());
        assertEquals(4, map.get(key));

        map.put(key, 6);

        for (int i = 0; i < gridCount(); ++i) {
            System.out.println("After put + get + remove + get + put + get: Grid " + i +
                " is primary " + grid(0).affinity(null).isPrimary(grid(i).localNode(), key) +
                " is backup " + grid(0).affinity(null).isBackup(grid(i).localNode(), key));
            System.out.println("After put + get + remove + get + put + get: On heap "  + grid(i).cache(null).withSkipStore().localPeek(key, ONHEAP_PEEK_MODES));
            System.out.println("After put + get + remove + get + put + get: Off heap "  + grid(i).cache(null).withSkipStore().localPeek(key, CachePeekMode.OFFHEAP));
        }


        assertTrue(cacheSkipStore.remove(key));

        for (int i = 0; i < gridCount(); ++i) {
            System.out.println("After put + get + remove + get + put + get + remove: Grid " + i +
                " is primary " + grid(0).affinity(null).isPrimary(grid(i).localNode(), key) +
                " is backup " + grid(0).affinity(null).isBackup(grid(i).localNode(), key));
            System.out.println("After put + get + remove + get + put + get + remove: On heap "  + grid(i).cache(null).withSkipStore().localPeek(key, ONHEAP_PEEK_MODES));
            System.out.println("After put + get + remove + get + put + get + remove: Off heap "  + grid(i).cache(null).withSkipStore().localPeek(key, CachePeekMode.OFFHEAP));
        }

        assertNull(cacheSkipStore.get(key));
        assertEquals(6, cache.get(key).intValue());
        assertNotNull(map.get(key));*/
    }

}
