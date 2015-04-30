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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class GridCacheSizeTopologyChangedTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grids count */
    private static final int GRIDS_CNT = 10;

    /** Keys count */
    private static final int KEYS_CNT = 1_000_000;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(ATOMIC);/*

        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);*/

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setBackups(1);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }


    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed
     */
    public void testCacheSize() throws Exception {
        Ignite g0 = startGrid(0);

        final AtomicBoolean canceled = new AtomicBoolean();

        final Random rnd = new Random();

        final boolean[] status = new boolean[GRIDS_CNT];

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while(!canceled.get()) {
                    int idx = rnd.nextInt(GRIDS_CNT);

                    if (idx > 0) {
                        boolean state = status[idx];

                        if (state) {
                            System.out.println("!!! STOP GRID: " + idx);
                            stopGrid(idx);
                        }
                        else {
                            System.out.println("!!! START GRID:" + idx);

                            startGrid(idx);
                        }

                        status[idx] = !state;

                        U.sleep(3000);
                    }
                }
                return null;
            }
        });

        IgniteCache<Integer, Integer> cache = null;

        try {
            cache = g0.cache(null);

            for (int i = 0; i < KEYS_CNT; i++) {
                cache.put(i, 0);

                int size = cache.size();

                if (i % 1000 == 0)
                    System.out.println("!!! Keys added: " + i + ", size: " + size);



                if (i % 1000 == 0) {
                    U.sleep(1000);
                    System.out.println("!!! Keys added: " + i + ", size: " + cache.size());
                }
            }

            canceled.set(true);
        }
        catch (Exception e) {
            System.out.println("!!! Terminated abnormally");
            e.printStackTrace();
        }
        finally {
            canceled.set(true);

            if (cache != null)
                assertEquals(KEYS_CNT, cache.size());

            fut.get();
        }
    }

}
