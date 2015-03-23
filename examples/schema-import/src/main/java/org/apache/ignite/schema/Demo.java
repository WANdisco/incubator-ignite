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

package org.apache.ignite.schema;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;

import javax.cache.configuration.*;

/**
 * Demo for CacheJdbcPojoStore.
 */
public class Demo {
    public static void main(String[] args) throws IgniteException {
        System.out.println(">>> Start demo...");

        IgniteConfiguration cfg = new IgniteConfiguration();

        CacheConfiguration ccfg = new CacheConfiguration<>();

        // Configure cache store.
        ccfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(ConfigurationSnippet.store()));
        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);

        // Enable database batching.
        ccfg.setWriteBehindEnabled(true);

        // Configure cache types metadata.
        ccfg.setTypeMetadata(ConfigurationSnippet.typeMetadata());

        cfg.setCacheConfiguration(ccfg);

        // Start Ignite node.
        try (Ignite ignite = Ignition.start(cfg)) {
            IgniteCache<PersonKey, Person> cache = ignite.jcache(null);

            // Demo for load cache with custom SQL.
            cache.loadCache(null, "org.apache.ignite.examples.demo.PersonKey",
                "select * from PERSON where ID = 3");

            System.out.println(">>> Loaded Person: " + cache.get(new PersonKey(3)));
        }
    }
}