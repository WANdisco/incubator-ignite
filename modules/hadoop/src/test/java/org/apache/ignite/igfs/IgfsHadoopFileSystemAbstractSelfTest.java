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

package org.apache.ignite.igfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.igfs.hadoop.*;
import org.apache.ignite.igfs.hadoop.v1.IgfsHadoopFileSystem;
import org.apache.ignite.internal.igfs.hadoop.*;
import org.apache.ignite.internal.processors.igfs.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.communication.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.igfs.IgfsMode.*;

/**
 * Test hadoop file system implementation.
 */
@SuppressWarnings("all")
public abstract class IgfsHadoopFileSystemAbstractSelfTest extends IgfsCommonAbstractTest {
    /** Primary file system authority. */
    private static final String PRIMARY_AUTHORITY = "igfs:grid0@";

    /** Primary file systme URI. */
    private static final String PRIMARY_URI = "igfs://" + PRIMARY_AUTHORITY + "/";

    /** Secondary file system authority. */
    private static final String SECONDARY_AUTHORITY = "igfs_secondary:grid_secondary@127.0.0.1:11500";

    /** Secondary file systme URI. */
    private static final String SECONDARY_URI = "igfs://" + SECONDARY_AUTHORITY + "/";

    /** Secondary file system configuration path. */
    private static final String SECONDARY_CFG_PATH = "/work/core-site-test.xml";

    /** Secondary endpoint configuration. */
    protected static final Map<String, String> SECONDARY_ENDPOINT_CFG = new HashMap<String, String>() {{
        put("type", "tcp");
        put("port", "11500");
    }};

    /** Group size. */
    public static final int GRP_SIZE = 128;

    /** Path to the default hadoop configuration. */
    public static final String HADOOP_FS_CFG = "examples/config/filesystem/core-site.xml";

    /** Thread count for multithreaded tests. */
    private static final int THREAD_CNT = 8;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Barrier for multithreaded tests. */
    private static CyclicBarrier barrier;

    /** File system. */
    private static FileSystem fs;

    /** Default IGFS mode. */
    protected final IgfsMode mode;

    /** Skip embedded mode flag. */
    private final boolean skipEmbed;

    /** Skip local shmem flag. */
    private final boolean skipLocShmem;

    /** Endpoint. */
    private final String endpoint;

    /** Primary file system URI. */
    protected URI primaryFsUri;

    /** Primary file system configuration. */
    protected Configuration primaryFsCfg;

    /** File statuses comparator. */
    private static final Comparator<FileStatus> STATUS_COMPARATOR = new Comparator<FileStatus>() {
        @SuppressWarnings("deprecation")
        @Override public int compare(FileStatus o1, FileStatus o2) {
            if (o1 == null || o2 == null)
                return o1 == o2 ? 0 : o1 == null ? -1 : 1;

            return o1.isDir() == o2.isDir() ? o1.getPath().compareTo(o2.getPath()) : o1.isDir() ? -1 : 1;
        }
    };

    /**
     * Constructor.
     *
     * @param mode Default IGFS mode.
     * @param skipEmbed Whether to skip embedded mode.
     * @param skipLocShmem Whether to skip local shmem mode.
     * @param skipLocTcp Whether to skip local TCP mode.
     */
    protected IgfsHadoopFileSystemAbstractSelfTest(IgfsMode mode, boolean skipEmbed, boolean skipLocShmem) {
        this.mode = mode;
        this.skipEmbed = skipEmbed;
        this.skipLocShmem = skipLocShmem;

        endpoint = skipLocShmem ? "127.0.0.1:10500" : "shmem:10500";
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Configuration secondaryConf = configuration(SECONDARY_AUTHORITY, true, true);

        secondaryConf.setInt("fs.igfs.block.size", 1024);

        String path = U.getIgniteHome() + SECONDARY_CFG_PATH;

        File file = new File(path);

        try (FileOutputStream fos = new FileOutputStream(file)) {
            secondaryConf.writeXml(fos);
        }

        startNodes();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }

    /**
     * Starts the nodes for this test.
     *
     * @throws Exception If failed.
     */
    private void startNodes() throws Exception {
        if (mode != PRIMARY) {
            // Start secondary IGFS.
            IgfsConfiguration igfsCfg = new IgfsConfiguration();

            igfsCfg.setDataCacheName("partitioned");
            igfsCfg.setMetaCacheName("replicated");
            igfsCfg.setName("igfs_secondary");
            igfsCfg.setIpcEndpointConfiguration(SECONDARY_ENDPOINT_CFG);
            igfsCfg.setBlockSize(512 * 1024);
            igfsCfg.setPrefetchBlocks(1);

            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setName("partitioned");
            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setDistributionMode(CacheDistributionMode.PARTITIONED_ONLY);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(GRP_SIZE));
            cacheCfg.setBackups(0);
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

            metaCacheCfg.setName("replicated");
            metaCacheCfg.setCacheMode(REPLICATED);
            metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.setGridName("grid_secondary");

            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

            cfg.setDiscoverySpi(discoSpi);
            cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
            cfg.setIgfsConfiguration(igfsCfg);
            cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

            cfg.setCommunicationSpi(communicationSpi());

            G.start(cfg);
        }

        startGrids(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stopAll(true);

        String path = U.getIgniteHome() + SECONDARY_CFG_PATH;

        new File(path).delete();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        primaryFsUri = new URI(PRIMARY_URI);

        primaryFsCfg = configuration(PRIMARY_AUTHORITY, skipEmbed, skipLocShmem);

        fs = FileSystem.get(primaryFsUri, primaryFsCfg);

        barrier = new CyclicBarrier(THREAD_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            fs.delete(new Path("/"), true);
        }
        catch (Exception ignore) {
            // No-op.
        }

        U.closeQuiet(fs);
    }

    /**
     * Get primary IPC endpoint configuration.
     *
     * @param gridName Grid name.
     * @return IPC primary endpoint configuration.
     */
    protected abstract Map<String, String> primaryIpcEndpointConfiguration(String gridName);

    /** {@inheritDoc} */
    @Override public String getTestGridName() {
        return "grid";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(cacheConfiguration(gridName));
        cfg.setIgfsConfiguration(igfsConfiguration(gridName));
        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);
        cfg.setCommunicationSpi(communicationSpi());

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    protected CacheConfiguration[] cacheConfiguration(String gridName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(CacheDistributionMode.PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(GRP_SIZE));
        cacheCfg.setBackups(0);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        return new CacheConfiguration[] {metaCacheCfg, cacheCfg};
    }

    /**
     * Gets IGFS configuration.
     *
     * @param gridName Grid name.
     * @return IGFS configuration.
     */
    protected IgfsConfiguration igfsConfiguration(String gridName) throws IgniteCheckedException {
        IgfsConfiguration cfg = new IgfsConfiguration();

        cfg.setDataCacheName("partitioned");
        cfg.setMetaCacheName("replicated");
        cfg.setName("igfs");
        cfg.setPrefetchBlocks(1);
        cfg.setDefaultMode(mode);

        if (mode != PRIMARY)
            cfg.setSecondaryFileSystem(new IgfsHadoopFileSystemWrapper(SECONDARY_URI, SECONDARY_CFG_PATH));

        cfg.setIpcEndpointConfiguration(primaryIpcEndpointConfiguration(gridName));

        cfg.setManagementPort(-1);
        cfg.setBlockSize(512 * 1024); // Together with group blocks mapper will yield 64M per node groups.

        return cfg;
    }

    /** @return Communication SPI. */
    private CommunicationSpi communicationSpi() {
        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        return commSpi;
    }

    /** @throws Exception If failed. */
    public void testGetUriIfFSIsNotInitialized() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return new IgfsHadoopFileSystem().getUri();
            }
        }, IllegalStateException.class, "URI is null (was IgfsHadoopFileSystem properly initialized?).");
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("NullableProblems")
    public void testInitializeCheckParametersNameIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                new IgfsHadoopFileSystem().initialize(null, new Configuration());

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: name");
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("NullableProblems")
    public void testInitializeCheckParametersCfgIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                new IgfsHadoopFileSystem().initialize(new URI(""), null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: cfg");
    }

    /** @throws Exception If failed. */
    public void testInitialize() throws Exception {
        final IgfsHadoopFileSystem fs = new IgfsHadoopFileSystem();

        fs.initialize(primaryFsUri, primaryFsCfg);

        // Check repeatable initialization.
        try {
            fs.initialize(primaryFsUri, primaryFsCfg);

            fail();
        }
        catch (IOException e) {
            assertTrue(e.getMessage().contains("File system is already initialized"));
        }

        assertEquals(primaryFsUri, fs.getUri());

        assertEquals(0, fs.getUsed());

        fs.close();
    }

    /**
     * Test how IPC cache map works.
     *
     * @throws Exception If failed.
     */
    public void testIpcCache() throws Exception {
        IgfsHadoopEx hadoop = GridTestUtils.getFieldValue(fs, "rmtClient", "delegateRef", "value", "hadoop");

        if (hadoop instanceof IgfsHadoopOutProc) {
            FileSystem fsOther = null;

            try {
                Field field = IgfsHadoopIpcIo.class.getDeclaredField("ipcCache");

                field.setAccessible(true);

                Map<String, IgfsHadoopIpcIo> cache = (Map<String, IgfsHadoopIpcIo>)field.get(null);

                Configuration cfg = configuration(PRIMARY_AUTHORITY, skipEmbed, skipLocShmem);

                // we disable caching in order to obtain new FileSystem instance.
                cfg.setBoolean("fs.igfs.impl.disable.cache", true);

                // Initial cache size.
                int initSize = cache.size();

                // Ensure that when IO is used by multiple file systems and one of them is closed, IO is not stopped.
                fsOther = FileSystem.get(new URI(PRIMARY_URI), cfg);

                assert fs != fsOther;

                assertEquals(initSize, cache.size());

                fsOther.close();

                assertEquals(initSize, cache.size());

                Field stopField = IgfsHadoopIpcIo.class.getDeclaredField("stopping");

                stopField.setAccessible(true);

                IgfsHadoopIpcIo io = null;

                for (Map.Entry<String, IgfsHadoopIpcIo> ioEntry : cache.entrySet()) {
                    if (endpoint.contains(ioEntry.getKey())) {
                        io = ioEntry.getValue();

                        break;
                    }
                }

                assert io != null;

                assert !(Boolean)stopField.get(io);

                // Ensure that IO is stopped when nobody else is need it.
                fs.close();

                assertEquals(initSize - 1, cache.size());

                assert (Boolean)stopField.get(io);
            }
            finally {
                U.closeQuiet(fsOther);
            }
        }
    }

    /** @throws Exception If failed. */
    public void testCloseIfNotInitialized() throws Exception {
        final FileSystem fs = new IgfsHadoopFileSystem();

        // Check close makes nothing harmful.
        fs.close();
    }

    /** @throws Exception If failed. */
    public void testClose() throws Exception {
        final Path path = new Path("dir");

        fs.close();

        // Check double close makes nothing harmful.
        fs.close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                fs.initialize(primaryFsUri, primaryFsCfg);

                return null;
            }
        }, IOException.class, "File system is stopped.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                fs.setPermission(path, FsPermission.createImmutable((short)777));

                return null;
            }
        }, IOException.class, "File system is stopped.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                fs.setOwner(path, "user", "group");

                return null;
            }
        }, IOException.class, "File system is stopped.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.open(path, 256);
            }
        }, IOException.class, "File system is stopped.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.create(path);
            }
        }, IOException.class, "File system is stopped.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.append(path);
            }
        }, IOException.class, "File system is stopped.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.rename(path, new Path("newDir"));
            }
        }, IOException.class, "File system is stopped.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.delete(path, true);
            }
        }, IOException.class, "File system is stopped.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.listStatus(path);
            }
        }, IOException.class, "File system is stopped.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.mkdirs(path);
            }
        }, IOException.class, "File system is stopped.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.getFileStatus(path);
            }
        }, IOException.class, "File system is stopped.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.getFileBlockLocations(new FileStatus(1L, false, 1, 1L, 1L, new Path("path")), 0L, 256L);
            }
        }, IOException.class, "File system is stopped.");
    }

    /** @throws Exception If failed. */
    public void testCreateCheckParameters() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.create(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("deprecation")
    public void testCreateBase() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path dir = new Path(fsHome, "/someDir1/someDir2/someDir3");
        Path file = new Path(dir, "someFile");

        assertPathDoesNotExist(fs, file);

        FsPermission fsPerm = new FsPermission((short)644);

        FSDataOutputStream os = fs.create(file, fsPerm, false, 1, (short)1, 1L, null);

        // Try to write something in file.
        os.write("abc".getBytes());

        os.close();

        // Check file status.
        FileStatus fileStatus = fs.getFileStatus(file);

        assertFalse(fileStatus.isDir());
        assertEquals(file, fileStatus.getPath());
        assertEquals(fsPerm, fileStatus.getPermission());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("deprecation")
    public void testCreateCheckOverwrite() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path dir = new Path(fsHome, "/someDir1/someDir2/someDir3");
        final Path file = new Path(dir, "someFile");

        FSDataOutputStream out = fs.create(file, FsPermission.getDefault(), false, 64 * 1024,
            fs.getDefaultReplication(), fs.getDefaultBlockSize(), null);

        out.close();

        // Check intermediate directory permissions.
        assertEquals(FsPermission.getDefault(), fs.getFileStatus(dir).getPermission());
        assertEquals(FsPermission.getDefault(), fs.getFileStatus(dir.getParent()).getPermission());
        assertEquals(FsPermission.getDefault(), fs.getFileStatus(dir.getParent().getParent()).getPermission());

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.create(file, FsPermission.getDefault(), false, 1024, (short)1, 2048, null);
            }
        }, PathExistsException.class, null);

        // Overwrite should be successful.
        FSDataOutputStream out1 = fs.create(file, true);

        out1.close();
    }

    /** @throws Exception If failed. */
    public void testDeleteIfNoSuchPath() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path dir = new Path(fsHome, "/someDir1/someDir2/someDir3");

        assertPathDoesNotExist(fs, dir);

        assertFalse(fs.delete(dir, true));
    }

    /** @throws Exception If failed. */
    public void testDeleteSuccessfulIfPathIsOpenedToRead() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "myFile");

        FSDataOutputStream os = fs.create(file, false, 128);

        final int cnt = 5 * IgfsConfiguration.DFLT_BLOCK_SIZE; // Write 5 blocks.

        for (int i = 0; i < cnt; i++)
            os.writeInt(i);

        os.close();

        final FSDataInputStream is = fs.open(file, -1);

        for (int i = 0; i < cnt / 2; i++)
            assertEquals(i, is.readInt());

        assert fs.delete(file, false);

        assert !fs.exists(file);

        is.close();
    }

    /** @throws Exception If failed. */
    public void testDeleteIfFilePathExists() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "myFile");

        FSDataOutputStream os = fs.create(file);

        os.close();

        assertTrue(fs.delete(file, false));

        assertPathDoesNotExist(fs, file);
    }

    /** @throws Exception If failed. */
    public void testDeleteIfDirectoryPathExists() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path dir = new Path(fsHome, "/someDir1/someDir2/someDir3");

        FSDataOutputStream os = fs.create(dir);

        os.close();

        assertTrue(fs.delete(dir, false));

        assertPathDoesNotExist(fs, dir);
    }

    /** @throws Exception If failed. */
    public void testDeleteFailsIfNonRecursive() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path someDir3 = new Path(fsHome, "/someDir1/someDir2/someDir3");

        fs.create(someDir3).close();

        Path someDir2 = new Path(fsHome, "/someDir1/someDir2");

        assertFalse(fs.delete(someDir2, false));

        assertPathExists(fs, someDir2);
        assertPathExists(fs, someDir3);
    }

    /** @throws Exception If failed. */
    public void testDeleteRecursively() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path someDir3 = new Path(fsHome, "/someDir1/someDir2/someDir3");

        FSDataOutputStream os = fs.create(someDir3);

        os.close();

        Path someDir2 = new Path(fsHome, "/someDir1/someDir2");

        assertTrue(fs.delete(someDir2, true));

        assertPathDoesNotExist(fs, someDir2);
        assertPathDoesNotExist(fs, someDir3);
    }

    /** @throws Exception If failed. */
    public void testDeleteRecursivelyFromRoot() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path someDir3 = new Path(fsHome, "/someDir1/someDir2/someDir3");

        FSDataOutputStream os = fs.create(someDir3);

        os.close();

        Path root = new Path(fsHome, "/");

        assertTrue(fs.delete(root, true));

        assertPathDoesNotExist(fs, someDir3);
        assertPathDoesNotExist(fs, new Path(fsHome, "/someDir1/someDir2"));
        assertPathDoesNotExist(fs, new Path(fsHome, "/someDir1"));
        assertPathExists(fs, root);
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("deprecation")
    public void testSetPermissionCheckDefaultPermission() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file, FsPermission.getDefault(), false, 64 * 1024,
            fs.getDefaultReplication(), fs.getDefaultBlockSize(), null);

        os.close();

        fs.setPermission(file, null);

        assertEquals(FsPermission.getDefault(), fs.getFileStatus(file).getPermission());
        assertEquals(FsPermission.getDefault(), fs.getFileStatus(file.getParent()).getPermission());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("deprecation")
    public void testSetPermissionCheckNonRecursiveness() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file, FsPermission.getDefault(), false, 64 * 1024,
            fs.getDefaultReplication(), fs.getDefaultBlockSize(), null);

        os.close();

        Path tmpDir = new Path(fsHome, "/tmp");

        FsPermission perm = new FsPermission((short)123);

        fs.setPermission(tmpDir, perm);

        assertEquals(perm, fs.getFileStatus(tmpDir).getPermission());
        assertEquals(FsPermission.getDefault(), fs.getFileStatus(file).getPermission());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("OctalInteger")
    public void testSetPermission() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file);

        os.close();

        for (short i = 0; i <= 0777; i += 7) {
            FsPermission perm = new FsPermission(i);

            fs.setPermission(file, perm);

            assertEquals(perm, fs.getFileStatus(file).getPermission());
        }
    }

    /** @throws Exception If failed. */
    public void testSetPermissionIfOutputStreamIsNotClosed() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "myFile");

        FsPermission perm = new FsPermission((short)123);

        FSDataOutputStream os = fs.create(file);

        fs.setPermission(file, perm);

        os.close();

        assertEquals(perm, fs.getFileStatus(file).getPermission());
    }

    /** @throws Exception If failed. */
    public void testSetOwnerCheckParametersPathIsNull() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file);

        os.close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.setOwner(null, "aUser", "aGroup");

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: p");
    }

    /** @throws Exception If failed. */
    public void testSetOwnerCheckParametersUserIsNull() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file);

        os.close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.setOwner(file, null, "aGroup");

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: username");
    }

    /** @throws Exception If failed. */
    public void testSetOwnerCheckParametersGroupIsNull() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file);

        os.close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                fs.setOwner(file, "aUser", null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: grpName");
    }

    /** @throws Exception If failed. */
    public void testSetOwner() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file);

        os.close();

        fs.setOwner(file, "aUser", "aGroup");

        assertEquals("aUser", fs.getFileStatus(file).getOwner());
        assertEquals("aGroup", fs.getFileStatus(file).getGroup());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetTimes() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "/heartbeat");

        fs.create(file).close();

        FileStatus status = fs.getFileStatus(file);

        assertTrue(status.getAccessTime() > 0);
        assertTrue(status.getModificationTime() > 0);

        long mtime = System.currentTimeMillis() - 5000;
        long atime = System.currentTimeMillis() - 4000;

        fs.setTimes(file, mtime, atime);

        status = fs.getFileStatus(file);

        assertEquals(mtime, status.getModificationTime());
        assertEquals(atime, status.getAccessTime());

        mtime -= 5000;

        fs.setTimes(file, mtime, -1);

        status = fs.getFileStatus(file);

        assertEquals(mtime, status.getModificationTime());
        assertEquals(atime, status.getAccessTime());

        atime -= 5000;

        fs.setTimes(file, -1, atime);

        status = fs.getFileStatus(file);

        assertEquals(mtime, status.getModificationTime());
        assertEquals(atime, status.getAccessTime());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetOwnerIfOutputStreamIsNotClosed() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "myFile");

        FSDataOutputStream os = fs.create(file);

        fs.setOwner(file, "aUser", "aGroup");

        os.close();

        assertEquals("aUser", fs.getFileStatus(file).getOwner());
        assertEquals("aGroup", fs.getFileStatus(file).getGroup());
    }

    /** @throws Exception If failed. */
    public void testSetOwnerCheckNonRecursiveness() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file);

        os.close();

        Path tmpDir = new Path(fsHome, "/tmp");

        fs.setOwner(file, "fUser", "fGroup");
        fs.setOwner(tmpDir, "dUser", "dGroup");

        assertEquals("dUser", fs.getFileStatus(tmpDir).getOwner());
        assertEquals("dGroup", fs.getFileStatus(tmpDir).getGroup());

        assertEquals("fUser", fs.getFileStatus(file).getOwner());
        assertEquals("fGroup", fs.getFileStatus(file).getGroup());
    }

    /** @throws Exception If failed. */
    public void testOpenCheckParametersPathIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.open(null, 1024);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    public void testOpenNoSuchPath() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "someFile");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.open(file, 1024);
            }
        }, FileNotFoundException.class, null);
    }

    /** @throws Exception If failed. */
    public void testOpenIfPathIsAlreadyOpened() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "someFile");

        FSDataOutputStream os = fs.create(file);

        os.close();

        FSDataInputStream is1 = fs.open(file);
        FSDataInputStream is2 = fs.open(file);

        is1.close();
        is2.close();
    }

    /** @throws Exception If failed. */
    public void testOpen() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "someFile");

        int cnt = 2 * 1024;

        FSDataOutputStream out = fs.create(file, true, 1024);

        for (long i = 0; i < cnt; i++)
            out.writeLong(i);

        out.close();

        FSDataInputStream in = fs.open(file, 1024);

        for (long i = 0; i < cnt; i++)
            assertEquals(i, in.readLong());

        in.close();
    }

    /** @throws Exception If failed. */
    public void testAppendCheckParametersPathIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.append(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    public void testAppendIfPathPointsToDirectory() throws Exception {
        final Path fsHome = new Path(primaryFsUri);
        final Path dir = new Path(fsHome, "/tmp");
        Path file = new Path(dir, "my");

        FSDataOutputStream os = fs.create(file);

        os.close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.append(new Path(fsHome, dir), 1024);
            }
        }, IOException.class, null);
    }

    /** @throws Exception If failed. */
    public void testAppendIfFileIsAlreadyBeingOpenedToWrite() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "someFile");

        FSDataOutputStream os = fs.create(file);

        os.close();

        FSDataOutputStream appendOs = fs.append(file);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return fs.append(file);
            }
        }, IOException.class, null);

        appendOs.close();
    }

    /** @throws Exception If failed. */
    public void testAppend() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "someFile");

        int cnt = 1024;

        FSDataOutputStream out = fs.create(file, true, 1024);

        for (int i = 0; i < cnt; i++)
            out.writeLong(i);

        out.close();

        out = fs.append(file);

        for (int i = cnt; i < cnt * 2; i++)
            out.writeLong(i);

        out.close();

        FSDataInputStream in = fs.open(file, 1024);

        for (int i = 0; i < cnt * 2; i++)
            assertEquals(i, in.readLong());

        in.close();
    }

    /** @throws Exception If failed. */
    public void testRenameCheckParametersSrcPathIsNull() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "someFile");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.rename(null, file);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: src");
    }

    /** @throws Exception If failed. */
    public void testRenameCheckParametersDstPathIsNull() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "someFile");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return fs.rename(file, null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: dst");
    }

    /** @throws Exception If failed. */
    public void testRenameIfSrcPathDoesNotExist() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path srcFile = new Path(fsHome, "srcFile");
        Path dstFile = new Path(fsHome, "dstFile");

        assertPathDoesNotExist(fs, srcFile);

        assertFalse(fs.rename(srcFile, dstFile));

        assertPathDoesNotExist(fs, dstFile);
    }

    /** @throws Exception If failed. */
    public void testRenameIfSrcPathIsAlreadyBeingOpenedToWrite() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path srcFile = new Path(fsHome, "srcFile");
        Path dstFile = new Path(fsHome, "dstFile");

        FSDataOutputStream os = fs.create(srcFile);

        os.close();

        os = fs.append(srcFile);

        assertTrue(fs.rename(srcFile, dstFile));

        assertPathExists(fs, dstFile);

        String testStr = "Test";

        try {
            os.writeBytes(testStr);
        }
        finally {
            os.close();
        }

        try (FSDataInputStream is = fs.open(dstFile)) {
            byte[] buf = new byte[testStr.getBytes().length];

            is.readFully(buf);

            assertEquals(testStr, new String(buf));
        }
    }

    /** @throws Exception If failed. */
    public void testRenameFileIfDstPathExists() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path srcFile = new Path(fsHome, "srcFile");
        Path dstFile = new Path(fsHome, "dstFile");

        FSDataOutputStream os = fs.create(srcFile);

        os.close();

        os = fs.create(dstFile);

        os.close();

        assertFalse(fs.rename(srcFile, dstFile));

        assertPathExists(fs, srcFile);
        assertPathExists(fs, dstFile);
    }

    /** @throws Exception If failed. */
    public void testRenameFile() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path srcFile = new Path(fsHome, "/tmp/srcFile");
        Path dstFile = new Path(fsHome, "/tmp/dstFile");

        FSDataOutputStream os = fs.create(srcFile);

        os.close();

        assertTrue(fs.rename(srcFile, dstFile));

        assertPathDoesNotExist(fs, srcFile);
        assertPathExists(fs, dstFile);
    }

    /** @throws Exception If failed. */
    public void testRenameIfSrcPathIsAlreadyBeingOpenedToRead() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path srcFile = new Path(fsHome, "srcFile");
        Path dstFile = new Path(fsHome, "dstFile");

        FSDataOutputStream os = fs.create(srcFile);

        int cnt = 1024;

        for (int i = 0; i < cnt; i++)
            os.writeInt(i);

        os.close();

        FSDataInputStream is = fs.open(srcFile);

        for (int i = 0; i < cnt; i++) {
            if (i == 100)
                // Rename file during the read process.
                assertTrue(fs.rename(srcFile, dstFile));

            assertEquals(i, is.readInt());
        }

        assertPathDoesNotExist(fs, srcFile);
        assertPathExists(fs, dstFile);

        os.close();
        is.close();
    }

    /** @throws Exception If failed. */
    public void testRenameDirectoryIfDstPathExists() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path srcDir = new Path(fsHome, "/tmp/");
        Path dstDir = new Path(fsHome, "/tmpNew/");

        FSDataOutputStream os = fs.create(new Path(srcDir, "file1"));

        os.close();

        os = fs.create(new Path(dstDir, "file2"));

        os.close();

        assertTrue("Rename succeeded [srcDir=" + srcDir + ", dstDir=" + dstDir + ']', fs.rename(srcDir, dstDir));

        assertPathExists(fs, dstDir);
        assertPathExists(fs, new Path(fsHome, "/tmpNew/tmp"));
        assertPathExists(fs, new Path(fsHome, "/tmpNew/tmp/file1"));
    }

    /** @throws Exception If failed. */
    public void testRenameDirectory() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path dir = new Path(fsHome, "/tmp/");
        Path newDir = new Path(fsHome, "/tmpNew/");

        FSDataOutputStream os = fs.create(new Path(dir, "myFile"));

        os.close();

        assertTrue("Rename failed [dir=" + dir + ", newDir=" + newDir + ']', fs.rename(dir, newDir));

        assertPathDoesNotExist(fs, dir);
        assertPathExists(fs, newDir);
    }

    /** @throws Exception If failed. */
    public void testListStatusIfPathIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.listStatus((Path)null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    public void testListStatusIfPathDoesNotExist() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return fs.listStatus(new Path("/tmp/some/dir"));
                }
            }, FileNotFoundException.class, null);
    }

    /**
     * Test directory listing.
     *
     * @throws Exception If failed.
     */
    public void testListStatus() throws Exception {
        Path igfsHome = new Path(PRIMARY_URI);

        // Test listing of an empty directory.
        Path dir = new Path(igfsHome, "dir");

        assert fs.mkdirs(dir);

        FileStatus[] list = fs.listStatus(dir);

        assert list.length == 0;

        // Test listing of a not empty directory.
        Path subDir = new Path(dir, "subDir");

        assert fs.mkdirs(subDir);

        Path file = new Path(dir, "file");

        FSDataOutputStream fos = fs.create(file);

        fos.close();

        list = fs.listStatus(dir);

        assert list.length == 2;

        String listRes1 = list[0].getPath().getName();
        String listRes2 = list[1].getPath().getName();

        assert "subDir".equals(listRes1) && "file".equals(listRes2) || "subDir".equals(listRes2) &&
            "file".equals(listRes1);

        // Test listing of a file.
        list = fs.listStatus(file);

        assert list.length == 1;

        assert "file".equals(list[0].getPath().getName());
    }

    /** @throws Exception If failed. */
    public void testSetWorkingDirectoryIfPathIsNull() throws Exception {
        fs.setWorkingDirectory(null);

        Path file = new Path("file");

        FSDataOutputStream os = fs.create(file);
        os.close();

        String path = fs.getFileStatus(file).getPath().toString();

        assertTrue(path.endsWith("/user/" + System.getProperty("user.name", "anonymous") + "/file"));
    }

    /** @throws Exception If failed. */
    public void testSetWorkingDirectoryIfPathDoesNotExist() throws Exception {
        // Should not throw any exceptions.
        fs.setWorkingDirectory(new Path("/someDir"));
    }

    /** @throws Exception If failed. */
    public void testSetWorkingDirectory() throws Exception {
        Path dir = new Path("/tmp/nested/dir");
        Path file = new Path("file");

        fs.mkdirs(dir);

        fs.setWorkingDirectory(dir);

        FSDataOutputStream os = fs.create(file);
        os.close();

        String filePath = fs.getFileStatus(new Path(dir, file)).getPath().toString();

        assertTrue(filePath.contains("/tmp/nested/dir/file"));
    }

    /** @throws Exception If failed. */
    public void testGetWorkingDirectoryIfDefault() throws Exception {
        String path = fs.getWorkingDirectory().toString();

        assertTrue(path.endsWith("/user/" + System.getProperty("user.name", "anonymous")));
    }

    /** @throws Exception If failed. */
    public void testGetWorkingDirectory() throws Exception {
        Path dir = new Path("/tmp/some/dir");

        fs.mkdirs(dir);

        fs.setWorkingDirectory(dir);

        String path = fs.getWorkingDirectory().toString();

        assertTrue(path.endsWith("/tmp/some/dir"));
    }

    /** @throws Exception If failed. */
    public void testMkdirsIfPathIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.mkdirs(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    public void testMkdirsIfPermissionIsNull() throws Exception {
        Path dir = new Path("/tmp");

        assertTrue(fs.mkdirs(dir, null));

        assertEquals(FsPermission.getDefault(), fs.getFileStatus(dir).getPermission());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("OctalInteger")
    public void testMkdirs() throws Exception {
        Path fsHome = new Path(PRIMARY_URI);
        Path dir = new Path(fsHome, "/tmp/staging");
        Path nestedDir = new Path(dir, "nested");

        FsPermission dirPerm = FsPermission.createImmutable((short)0700);
        FsPermission nestedDirPerm = FsPermission.createImmutable((short)111);

        assertTrue(fs.mkdirs(dir, dirPerm));
        assertTrue(fs.mkdirs(nestedDir, nestedDirPerm));

        assertEquals(dirPerm, fs.getFileStatus(dir).getPermission());
        assertEquals(nestedDirPerm, fs.getFileStatus(nestedDir).getPermission());
    }

    /** @throws Exception If failed. */
    public void testGetFileStatusIfPathIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.getFileStatus(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    public void testGetFileStatusIfPathDoesNotExist() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.getFileStatus(new Path("someDir"));
            }
        }, FileNotFoundException.class, "File not found: someDir");
    }

    /** @throws Exception If failed. */
    public void testGetFileBlockLocationsIfFileStatusIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                // Argument is checked by Hadoop.
                return fs.getFileBlockLocations((Path)null, 1, 2);
            }
        }, NullPointerException.class, null);
    }

    /** @throws Exception If failed. */
    public void testGetFileBlockLocationsIfFileStatusReferenceNotExistingPath() throws Exception {
        Path path = new Path("someFile");

        fs.create(path).close();

        final FileStatus status = fs.getFileStatus(path);

        fs.delete(path, true);

        BlockLocation[] locations = fs.getFileBlockLocations(status, 1, 2);

        assertEquals(0, locations.length);
    }

    /** @throws Exception If failed. */
    public void testGetFileBlockLocations() throws Exception {
        Path igfsHome = new Path(PRIMARY_URI);

        Path file = new Path(igfsHome, "someFile");

        try (OutputStream out = new BufferedOutputStream(fs.create(file, true, 1024 * 1024))) {
            byte[] data = new byte[128 * 1024];

            for (int i = 0; i < 100; i++)
                out.write(data);

            out.flush();
        }

        try (FSDataInputStream in = fs.open(file, 1024 * 1024)) {
            byte[] data = new byte[128 * 1024];

            int read;

            do {
                read = in.read(data);
            }
            while (read > 0);
        }

        FileStatus status = fs.getFileStatus(file);

        int grpLen = 128 * 512 * 1024;

        int grpCnt = (int)((status.getLen() + grpLen - 1) / grpLen);

        BlockLocation[] locations = fs.getFileBlockLocations(status, 0, status.getLen());

        assertEquals(grpCnt, locations.length);
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("deprecation")
    public void testGetDefaultBlockSize() throws Exception {
        assertEquals(1L << 26, fs.getDefaultBlockSize());
    }

    /** @throws Exception If failed. */
    public void testZeroReplicationFactor() throws Exception {
        // This test doesn't make sense for any mode except of PRIMARY.
        if (mode == PRIMARY) {
            Path igfsHome = new Path(PRIMARY_URI);

            Path file = new Path(igfsHome, "someFile");

            try (FSDataOutputStream out = fs.create(file, (short)0)) {
                out.write(new byte[1024 * 1024]);
            }

            IgniteFs igfs = grid(0).fileSystem("igfs");

            IgfsPath filePath = new IgfsPath("/someFile");

            IgfsFile fileInfo = igfs.info(filePath);

            Collection<IgfsBlockLocation> locations = igfs.affinity(filePath, 0, fileInfo.length());

            assertEquals(1, locations.size());

            IgfsBlockLocation location = F.first(locations);

            assertEquals(1, location.nodeIds().size());
        }
    }

    /**
     * Ensure that when running in multithreaded mode only one create() operation succeed.
     *
     * @throws Exception If failed.
     */
    public void testMultithreadedCreate() throws Exception {
        Path dir = new Path(new Path(PRIMARY_URI), "/dir");

        assert fs.mkdirs(dir);

        final Path file = new Path(dir, "file");

        fs.create(file).close();

        final AtomicInteger cnt = new AtomicInteger();

        final Collection<Integer> errs = new GridConcurrentHashSet<>(THREAD_CNT, 1.0f, THREAD_CNT);

        final AtomicBoolean err = new AtomicBoolean();

        multithreaded(new Runnable() {
            @Override
            public void run() {
                int idx = cnt.getAndIncrement();

                byte[] data = new byte[256];

                Arrays.fill(data, (byte)idx);

                FSDataOutputStream os = null;

                try {
                    os = fs.create(file, true);
                }
                catch (IOException ignore) {
                    errs.add(idx);
                }

                U.awaitQuiet(barrier);

                try {
                    if (os != null)
                        os.write(data);
                }
                catch (IOException ignore) {
                    err.set(true);
                }
                finally {
                    U.closeQuiet(os);
                }
            }
        }, THREAD_CNT);

        assert !err.get();

        // Only one thread could obtain write lock on the file.
        assert errs.size() == THREAD_CNT - 1;

        int idx = -1;

        for (int i = 0; i < THREAD_CNT; i++) {
            if (!errs.remove(i)) {
                idx = i;

                break;
            }
        }

        byte[] expData = new byte[256];

        Arrays.fill(expData, (byte)idx);

        FSDataInputStream is = fs.open(file);

        byte[] data = new byte[256];

        is.read(data);

        is.close();

        assert Arrays.equals(expData, data) : "Expected=" + Arrays.toString(expData) + ", actual=" +
            Arrays.toString(data);
    }

    /**
     * Ensure that when running in multithreaded mode only one append() operation succeed.
     *
     * @throws Exception If failed.
     */
    public void testMultithreadedAppend() throws Exception {
        Path dir = new Path(new Path(PRIMARY_URI), "/dir");

        assert fs.mkdirs(dir);

        final Path file = new Path(dir, "file");

        fs.create(file).close();

        final AtomicInteger cnt = new AtomicInteger();

        final Collection<Integer> errs = new GridConcurrentHashSet<>(THREAD_CNT, 1.0f, THREAD_CNT);

        final AtomicBoolean err = new AtomicBoolean();

        multithreaded(new Runnable() {
            @Override public void run() {
                int idx = cnt.getAndIncrement();

                byte[] data = new byte[256];

                Arrays.fill(data, (byte)idx);

                U.awaitQuiet(barrier);

                FSDataOutputStream os = null;

                try {
                    os = fs.append(file);
                }
                catch (IOException ignore) {
                    errs.add(idx);
                }

                U.awaitQuiet(barrier);

                try {
                    if (os != null)
                        os.write(data);
                }
                catch (IOException ignore) {
                    err.set(true);
                }
                finally {
                    U.closeQuiet(os);
                }
            }
        }, THREAD_CNT);

        assert !err.get();

        // Only one thread could obtain write lock on the file.
        assert errs.size() == THREAD_CNT - 1;

        int idx = -1;

        for (int i = 0; i < THREAD_CNT; i++) {
            if (!errs.remove(i)) {
                idx = i;

                break;
            }
        }

        byte[] expData = new byte[256];

        Arrays.fill(expData, (byte)idx);

        FSDataInputStream is = fs.open(file);

        byte[] data = new byte[256];

        is.read(data);

        is.close();

        assert Arrays.equals(expData, data);
    }

    /**
     * Test concurrent reads within the file.
     *
     * @throws Exception If failed.
     */
    public void testMultithreadedOpen() throws Exception {
        final byte[] dataChunk = new byte[256];

        for (int i = 0; i < dataChunk.length; i++)
            dataChunk[i] = (byte)i;

        Path dir = new Path(new Path(PRIMARY_URI), "/dir");

        assert fs.mkdirs(dir);

        final Path file = new Path(dir, "file");

        FSDataOutputStream os = fs.create(file);

        // Write 256 * 2048 = 512Kb of data.
        for (int i = 0; i < 2048; i++)
            os.write(dataChunk);

        os.close();

        final AtomicBoolean err = new AtomicBoolean();

        multithreaded(new Runnable() {
            @Override
            public void run() {
                FSDataInputStream is = null;

                try {
                    int pos = ThreadLocalRandom8.current().nextInt(2048);

                    try {
                        is = fs.open(file);
                    }
                    finally {
                        U.awaitQuiet(barrier);
                    }

                    is.seek(256 * pos);

                    byte[] buf = new byte[256];

                    for (int i = pos; i < 2048; i++) {
                        // First perform normal read.
                        int read = is.read(buf);

                        assert read == 256;

                        Arrays.equals(dataChunk, buf);
                    }

                    int res = is.read(buf);

                    assert res == -1;
                }
                catch (IOException ignore) {
                    err.set(true);
                }
                finally {
                    U.closeQuiet(is);
                }
            }
        }, THREAD_CNT);

        assert !err.get();
    }

    /**
     * Test concurrent creation of multiple directories.
     *
     * @throws Exception If failed.
     */
    public void testMultithreadedMkdirs() throws Exception {
        final Path dir = new Path(new Path(PRIMARY_URI), "/dir");

        assert fs.mkdirs(dir);

        final int depth = 3;
        final int entryCnt = 5;

        final AtomicReference<IOException> err = new AtomicReference();

        multithreaded(new Runnable() {
            @Override public void run() {
                Deque<IgniteBiTuple<Integer, Path>> queue = new ArrayDeque<>();

                queue.add(F.t(0, dir));

                U.awaitQuiet(barrier);

                while (!queue.isEmpty()) {
                    IgniteBiTuple<Integer, Path> t = queue.pollFirst();

                    int curDepth = t.getKey();
                    Path curPath = t.getValue();

                    if (curDepth <= depth) {
                        int newDepth = curDepth + 1;

                        // Create directories.
                        for (int i = 0; i < entryCnt; i++) {
                            Path subDir = new Path(curPath, "dir-" + newDepth + "-" + i);

                            try {
                                if (fs.mkdirs(subDir))
                                    queue.addLast(F.t(newDepth, subDir));
                            }
                            catch (IOException e) {
                                err.compareAndSet(null, e);
                            }
                        }
                    }
                }
            }
        }, THREAD_CNT);

        // Ensure there were no errors.
        assert err.get() == null : err.get();

        // Ensure correct folders structure.
        Deque<IgniteBiTuple<Integer, Path>> queue = new ArrayDeque<>();

        queue.add(F.t(0, dir));

        while (!queue.isEmpty()) {
            IgniteBiTuple<Integer, Path> t = queue.pollFirst();

            int curDepth = t.getKey();
            Path curPath = t.getValue();

            if (curDepth <= depth) {
                int newDepth = curDepth + 1;

                // Create directories.
                for (int i = 0; i < entryCnt; i++) {
                    Path subDir = new Path(curPath, "dir-" + newDepth + "-" + i);

                    assert fs.exists(subDir) : "Expected directory doesn't exist: " + subDir;

                    queue.add(F.t(newDepth, subDir));
                }
            }
        }
    }

    /**
     * Test concurrent deletion of the same directory with advanced structure.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("TooBroadScope")
    public void testMultithreadedDelete() throws Exception {
        final Path dir = new Path(new Path(PRIMARY_URI), "/dir");

        assert fs.mkdirs(dir);

        int depth = 3;
        int entryCnt = 5;

        Deque<IgniteBiTuple<Integer, Path>> queue = new ArrayDeque<>();

        queue.add(F.t(0, dir));

        while (!queue.isEmpty()) {
            IgniteBiTuple<Integer, Path> t = queue.pollFirst();

            int curDepth = t.getKey();
            Path curPath = t.getValue();

            if (curDepth < depth) {
                int newDepth = curDepth + 1;

                // Create directories.
                for (int i = 0; i < entryCnt; i++) {
                    Path subDir = new Path(curPath, "dir-" + newDepth + "-" + i);

                    fs.mkdirs(subDir);

                    queue.addLast(F.t(newDepth, subDir));
                }
            }
            else {
                // Create files.
                for (int i = 0; i < entryCnt; i++) {
                    Path file = new Path(curPath, "file " + i);

                    fs.create(file).close();
                }
            }
        }

        final AtomicBoolean err = new AtomicBoolean();

        multithreaded(new Runnable() {
            @Override public void run() {
                try {
                    U.awaitQuiet(barrier);

                    fs.delete(dir, true);
                }
                catch (IOException ignore) {
                    err.set(true);
                }
            }
        }, THREAD_CNT);

        // Ensure there were no errors.
        assert !err.get();

        // Ensure the directory was actually deleted.

        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    return !fs.exists(dir);
                }
                catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }, 5000L);
    }

    /** @throws Exception If failed. */
    public void testConsistency() throws Exception {
        // Default buffers values
        checkConsistency(-1, 1, -1, -1, 1, -1);
        checkConsistency(-1, 10, -1, -1, 10, -1);
        checkConsistency(-1, 100, -1, -1, 100, -1);
        checkConsistency(-1, 1000, -1, -1, 1000, -1);
        checkConsistency(-1, 10000, -1, -1, 10000, -1);
        checkConsistency(-1, 100000, -1, -1, 100000, -1);

        checkConsistency(65 * 1024 + 13, 100000, -1, -1, 100000, -1);

        checkConsistency(-1, 100000, 2 * 4 * 1024 + 17, -1, 100000, -1);

        checkConsistency(-1, 100000, -1, 65 * 1024 + 13, 100000, -1);

        checkConsistency(-1, 100000, -1, -1, 100000, 2 * 4 * 1024 + 17);

        checkConsistency(65 * 1024 + 13, 100000, 2 * 4 * 1024 + 13, 65 * 1024 + 149, 100000, 2 * 4 * 1024 + 157);
    }

    /**
     * Verifies that client reconnects after connection to the server has been lost.
     *
     * @throws Exception If error occurs.
     */
    public void testClientReconnect() throws Exception {
        Path filePath = new Path(PRIMARY_URI, "file1");

        final FSDataOutputStream s = fs.create(filePath); // Open the stream before stopping IGFS.

        try {
            G.stopAll(true); // Stop the server.

            startNodes(); // Start server again.

            // Check that client is again operational.
            assertTrue(fs.mkdirs(new Path(PRIMARY_URI, "dir1/dir2")));

            // However, the streams, opened before disconnect, should not be valid.
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    s.write("test".getBytes());

                    s.flush(); // Flush data to the broken output stream.

                    return null;
                }
            }, IOException.class, null);

            assertFalse(fs.exists(filePath));
        }
        finally {
            U.closeQuiet(s); // Safety.
        }
    }

    /**
     * Verifies that client reconnects after connection to the server has been lost (multithreaded mode).
     *
     * @throws Exception If error occurs.
     */
    public void testClientReconnectMultithreaded() throws Exception {
        final ConcurrentLinkedQueue<FileSystem> q = new ConcurrentLinkedQueue<>();

        Configuration cfg = new Configuration();

        for (Map.Entry<String, String> entry : primaryFsCfg)
            cfg.set(entry.getKey(), entry.getValue());

        cfg.setBoolean("fs.igfs.impl.disable.cache", true);

        final int nClients = 1;

        // Initialize clients.
        for (int i = 0; i < nClients; i++)
            q.add(FileSystem.get(primaryFsUri, cfg));

        G.stopAll(true); // Stop the server.

        startNodes(); // Start server again.

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                FileSystem fs = q.poll();

                try {
                    // Check that client is again operational.
                    assertTrue(fs.mkdirs(new Path("/" + Thread.currentThread().getName())));

                    return true;
                }
                finally {
                    U.closeQuiet(fs);
                }
            }
        }, nClients, "test-client");
    }

    /**
     * Checks consistency of create --> open --> append --> open operations with different buffer sizes.
     *
     * @param createBufSize Buffer size used for file creation.
     * @param writeCntsInCreate Count of times to write in file creation.
     * @param openAfterCreateBufSize Buffer size used for file opening after creation.
     * @param appendBufSize Buffer size used for file appending.
     * @param writeCntsInAppend Count of times to write in file appending.
     * @param openAfterAppendBufSize Buffer size used for file opening after appending.
     * @throws Exception If failed.
     */
    private void checkConsistency(int createBufSize, int writeCntsInCreate, int openAfterCreateBufSize,
        int appendBufSize, int writeCntsInAppend, int openAfterAppendBufSize) throws Exception {
        final Path igfsHome = new Path(PRIMARY_URI);

        Path file = new Path(igfsHome, "/someDir/someInnerDir/someFile");

        FSDataOutputStream os = fs.create(file, true, createBufSize);

        for (int i = 0; i < writeCntsInCreate; i++)
            os.writeInt(i);

        os.close();

        FSDataInputStream is = fs.open(file, openAfterCreateBufSize);

        for (int i = 0; i < writeCntsInCreate; i++)
            assertEquals(i, is.readInt());

        is.close();

        os = fs.append(file, appendBufSize);

        for (int i = writeCntsInCreate; i < writeCntsInCreate + writeCntsInAppend; i++)
            os.writeInt(i);

        os.close();

        is = fs.open(file, openAfterAppendBufSize);

        for (int i = 0; i < writeCntsInCreate + writeCntsInAppend; i++)
            assertEquals(i, is.readInt());

        is.close();
    }

    /**
     * Gets instance of Hadoop local file system.
     *
     * @param home File system home.
     * @return File system.
     * @throws IOException If failed.
     */
    private FileSystem local(Path home) throws IOException {
        Configuration cfg = new Configuration();

        cfg.addResource(U.resolveIgniteUrl(HADOOP_FS_CFG));

        return FileSystem.get(home.toUri(), cfg);
    }

    /**
     * Copy files from one FS to another.
     *
     * @param msg Info message to display after copying finishes.
     * @param srcFs Source file system.
     * @param src Source path to copy from.
     * @param destFs Destination file system.
     * @param dest Destination path to copy to.
     * @throws IOException If failed.
     */
    private void copy(String msg, FileSystem srcFs, Path src, FileSystem destFs, Path dest) throws IOException {
        assert destFs.delete(dest, true) || !destFs.exists(dest) : "Failed to remove: " + dest;

        destFs.mkdirs(dest);

        Configuration conf = new Configuration(true);

        long time = System.currentTimeMillis();

        FileUtil.copy(srcFs, src, destFs, dest, false, true, conf);

        time = System.currentTimeMillis() - time;

        info("Copying finished, " + msg + " [time=" + time + "ms, src=" + src + ", dest=" + dest + ']');
    }

    /**
     * Compare content of two folders.
     *
     * @param cfg Paths configuration to compare.
     * @throws IOException If failed.
     */
    @SuppressWarnings("deprecation")
    private void compareContent(Config cfg) throws IOException {
        Deque<Config> queue = new LinkedList<>();

        queue.add(cfg);

        for (Config c = queue.poll(); c != null; c = queue.poll()) {
            boolean exists;

            assertEquals("Check existence [src=" + c.src + ", dest=" + c.dest + ']',
                exists = c.srcFs.exists(c.src), c.destFs.exists(c.dest));

            assertEquals("Check types (files?) [src=" + c.src + ", dest=" + c.dest + ']',
                c.srcFs.isFile(c.src), c.destFs.isFile(c.dest));

            if (exists) {
                ContentSummary srcSummary = c.srcFs.getContentSummary(c.src);
                ContentSummary dstSummary = c.destFs.getContentSummary(c.dest);

                assertEquals("Directories number comparison failed",
                    srcSummary.getDirectoryCount(), dstSummary.getDirectoryCount());

                assertEquals("Files number comparison failed",
                    srcSummary.getFileCount(), dstSummary.getFileCount());

                assertEquals("Space consumed comparison failed",
                    srcSummary.getSpaceConsumed(), dstSummary.getSpaceConsumed());

                assertEquals("Length comparison failed",
                    srcSummary.getLength(), dstSummary.getLength());

                // Intentionally skipping quotas checks as they can vary.
            }
            else {
                assertContentSummaryFails(c.srcFs, c.src);
                assertContentSummaryFails(c.destFs, c.dest);
            }

            if (!exists)
                continue;

            FileStatus[] srcSt = c.srcFs.listStatus(c.src);
            FileStatus[] destSt = c.destFs.listStatus(c.dest);

            assert srcSt != null && destSt != null : "Both not null" +
                " [srcSt=" + Arrays.toString(srcSt) + ", destSt=" + Arrays.toString(destSt) + ']';

            assertEquals("Check listing [src=" + c.src + ", dest=" + c.dest + ']', srcSt.length, destSt.length);

            // Listing of the file returns the only element with this file.
            if (srcSt.length == 1 && c.src.equals(srcSt[0].getPath())) {
                assertEquals(c.dest, destSt[0].getPath());

                assertTrue("Expects file [src=" + c.src + ", srcSt[0]=" + srcSt[0] + ']', !srcSt[0].isDir());
                assertTrue("Expects file [dest=" + c.dest + ", destSt[0]=" + destSt[0] + ']', !destSt[0].isDir());

                FSDataInputStream srcIn = null;
                FSDataInputStream destIn = null;

                try {
                    srcIn = c.srcFs.open(c.src);
                    destIn = c.destFs.open(c.dest);

                    GridTestIoUtils.assertEqualStreams(srcIn, destIn, srcSt[0].getLen());
                }
                finally {
                    U.closeQuiet(srcIn);
                    U.closeQuiet(destIn);
                }

                continue; // Skip the following directories validations.
            }

            // Sort both arrays.
            Arrays.sort(srcSt, STATUS_COMPARATOR);
            Arrays.sort(destSt, STATUS_COMPARATOR);

            for (int i = 0; i < srcSt.length; i++)
                // Dig in deep to the last leaf, instead of collecting full tree in memory.
                queue.addFirst(new Config(c.srcFs, srcSt[i].getPath(), c.destFs, destSt[i].getPath()));

            // Add non-existent file to check in the current folder.
            String rndFile = "Non-existent file #" + UUID.randomUUID().toString();

            queue.addFirst(new Config(c.srcFs, new Path(c.src, rndFile), c.destFs, new Path(c.dest, rndFile)));
        }
    }

    /**
     * Test expected failures for 'close' operation.
     *
     * @param fs File system to test.
     * @param msg Expected exception message.
     */
    public void assertCloseFails(final FileSystem fs, String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                fs.close();

                return null;
            }
        }, IOException.class, msg);
    }

    /**
     * Test expected failures for 'get content summary' operation.
     *
     * @param fs File system to test.
     * @param path Path to evaluate content summary for.
     */
    private void assertContentSummaryFails(final FileSystem fs, final Path path) {
        GridTestUtils.assertThrows(log, new Callable<ContentSummary>() {
            @Override public ContentSummary call() throws Exception {
                return fs.getContentSummary(path);
            }
        }, FileNotFoundException.class, null);
    }

    /**
     * Assert that a given path exists in a given FileSystem.
     *
     * @param fs FileSystem to check.
     * @param p Path to check.
     * @throws IOException if the path does not exist.
     */
    private void assertPathExists(FileSystem fs, Path p) throws IOException {
        FileStatus fileStatus = fs.getFileStatus(p);

        assertEquals(p, fileStatus.getPath());
        assertNotSame(0, fileStatus.getModificationTime());
    }

    /**
     * Check path does not exist in a given FileSystem.
     *
     * @param fs FileSystem to check.
     * @param path Path to check.
     */
    private void assertPathDoesNotExist(final FileSystem fs, final Path path) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.getFileStatus(path);
            }
        }, FileNotFoundException.class, null);
    }

    /** Helper class to encapsulate source and destination folders. */
    @SuppressWarnings({"PublicInnerClass", "PublicField"})
    public static final class Config {
        /** Source file system. */
        public final FileSystem srcFs;

        /** Source path to work with. */
        public final Path src;

        /** Destination file system. */
        public final FileSystem destFs;

        /** Destination path to work with. */
        public final Path dest;

        /**
         * Copying task configuration.
         *
         * @param srcFs Source file system.
         * @param src Source path.
         * @param destFs Destination file system.
         * @param dest Destination path.
         */
        public Config(FileSystem srcFs, Path src, FileSystem destFs, Path dest) {
            this.srcFs = srcFs;
            this.src = src;
            this.destFs = destFs;
            this.dest = dest;
        }
    }

    /**
     * Convert path for exception message testing purposes.
     *
     * @param path Path.
     * @return Converted path.
     * @throws Exception If failed.
     */
    private Path convertPath(Path path) throws Exception {
        if (mode != PROXY)
            return path;
        else {
            URI secondaryUri = new URI(SECONDARY_URI);

            URI pathUri = path.toUri();

            return new Path(new URI(pathUri.getScheme() != null ? secondaryUri.getScheme() : null,
                pathUri.getAuthority() != null ? secondaryUri.getAuthority() : null, pathUri.getPath(), null, null));
        }
    }

    /**
     * Create configuration for test.
     *
     * @param authority Authority.
     * @param skipEmbed Whether to skip embedded mode.
     * @param skipLocShmem Whether to skip local shmem mode.
     * @return Configuration.
     */
    private static Configuration configuration(String authority, boolean skipEmbed, boolean skipLocShmem) {
        Configuration cfg = new Configuration();

        cfg.set("fs.defaultFS", "igfs://" + authority + "/");
        cfg.set("fs.igfs.impl", IgfsHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.igfs.impl",
            org.apache.ignite.igfs.hadoop.v2.IgfsHadoopFileSystem.class.getName());

        cfg.setBoolean("fs.igfs.impl.disable.cache", true);

        if (skipEmbed)
            cfg.setBoolean(String.format(IgfsHadoopUtils.PARAM_IGFS_ENDPOINT_NO_EMBED, authority), true);

        if (skipLocShmem)
            cfg.setBoolean(String.format(IgfsHadoopUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_SHMEM, authority), true);

        return cfg;
    }
}