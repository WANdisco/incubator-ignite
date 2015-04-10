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

package org.apache.ignite.multijvm;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Multi JVM tests. 
 */
public class MultiJvmTest extends GridCommonAbstractTest {
    /**
     * Runs given class as separate java process.
     *
     * @param runnerCls Runner class
     * @param b
     * @throws Exception If failed.
     */
    protected void runJavaProcess(Class<?> runnerCls, boolean wait) throws Exception {
        Process ps = GridJavaProcess.exec(
            runnerCls,
            null,
            null,
            null,
            null,
            Collections.<String>emptyList(),
            System.getProperty("surefire.test.class.path")
        ).getProcess();

        readStreams(ps);

        int code = 0;
        
        if (wait)
            code = ps.waitFor();

        assertEquals("Returned code have to be 0.", 0, code);
    }

    /**
     * Read information from process streams.
     *
     * @param proc Process.
     * @throws IOException If an I/O error occurs.
     */
    private void readStreams(final Process proc) throws IOException {
        Thread reader = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    BufferedReader stdOut = new BufferedReader(new InputStreamReader(proc.getInputStream()));

                    String s;

                    while ((s = stdOut.readLine()) != null)
                        System.out.println("OUT>>>>>> " + s);

                    BufferedReader errOut = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

                    while ((s = errOut.readLine()) != null)
                        System.out.println("ERR>>>>>> " + s);
                }
                catch (IOException e) {
                    // No-op.
                }
            }
        });
        
        reader.setDaemon(true);
        
        reader.start();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiNode() throws Exception {
        runJavaProcess(IgniteNodeRunner.class, false);
        
        log.info(">>>>> grids=" + Ignition.allGrids());

        Thread.sleep(15 * 1000);

        log.info(">>>>> grids=" + Ignition.allGrids());
        
        Ignition.stopAll(true);
        
        log.info(">>>>> grids=" + Ignition.allGrids());
    }
}