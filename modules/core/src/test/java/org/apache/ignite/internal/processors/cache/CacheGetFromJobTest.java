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
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.testframework.*;

import java.util.concurrent.atomic.*;

/**
 * Job try to get cache.
 */
public class CacheGetFromJobTest extends GridCacheAbstractSelfTest {
    private static final String CACHE_NAME = null;
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setName(CACHE_NAME);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopologyChange() throws Exception {
        final AtomicReference<Exception> err = new AtomicReference<>();

        final AtomicInteger id = new AtomicInteger(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
            @Override public void apply() {
                info("Run topology change.");

                try {
                    for (int i = 0; i < 5; i++) {
                        info("Topology change " + i);

                        startGrid(id.getAndIncrement());
                    }
                }
                catch (Exception e) {
                    err.set(e);

                    log.error("Unexpected exception in topology-change-thread: " + e, e);
                }
            }
        }, 3, "topology-change-thread");

        while (!fut.isDone())
            grid(0).compute().broadcast(new TestJob());

        Exception err0 = err.get();

        if (err0 != null)
            throw err0;
    }

    /**
     * Test job.
     */
    private static class TestJob implements IgniteCallable<Object> {
        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        public TestJob() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            assert ignite.cache(CACHE_NAME) != null;

            return null;
        }
    }
}
