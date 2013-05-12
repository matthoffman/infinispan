package org.infinispan.distexec.fj;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.jdk8backported.ForkJoinPool;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 *
 *
 */
@Test(groups = "functional", testName = "distexec.fj.TaskCachingDistributedForkJoinPoolTest")
public class TaskCachingDistributedForkJoinPoolTest extends MultipleCacheDistributedForkJoinPoolTest {
    //    static final Logger log = LoggerFactory.getLogger(DistributedForkJoinPoolTest.class);

    TaskCachingDistributedForkJoinPool[] pools;

    public TaskCachingDistributedForkJoinPoolTest() {
        cleanup = CleanupPhase.AFTER_METHOD;
    }

    EmbeddedCacheManager cacheManager;

    @AfterTest
    public void shutdownCache() {
        if (pools != null) {
            for (TaskCachingDistributedForkJoinPool pool : pools) {
                pool.shutdown();
            }
        }
        TestingUtil.killCacheManagers(cacheManager);
    }

    @Test
    public void sanityTest() throws Exception {
        // this isn't even testing our DistributedForkJoinPool; it's using a vanilla ForkJoinPool in order to test our test.
        ForkJoinPool plainForkJoinPool = new ForkJoinPool();
        long start = System.currentTimeMillis();
        final int end = 10000;
        Future<Long> sum = plainForkJoinPool.submit(new NormalFJSumTest(1, end, 10));
        log.info("The sum of the numbers from 1 to " + end + " is " + sum.get() + " (and it took "
                + (System.currentTimeMillis() - start) + " ms to calculate using a normal FJ pool)");
        assertEquals(Long.valueOf(44879160), sum.get());
    }

    @Test
    public void testBasic() throws Exception {
        createClusteredCaches(1, cacheName(), cb);
        for (EmbeddedCacheManager cacheManager : cacheManagers) {
            cacheManager.defineConfiguration(TaskCachingDistributedForkJoinPool.defaultTaskCacheConfigurationName,
                    cb.build());
        }

        TaskCachingDistributedForkJoinPool forkJoinPool = null;
        try {
            forkJoinPool = new TaskCachingDistributedForkJoinPool(cache(0, cacheName()));
            assertNotNull(forkJoinPool);
            long start = System.currentTimeMillis();
            final int end = 10000;
            Future<Long> sum = forkJoinPool.submit(new LocalSumTest(1, end, 10));
            log.info("The sum of the numbers from 1 to " + end + " is " + sum.get() + " (and it took "
                    + (System.currentTimeMillis() - start) + " ms to calculate)");
            assertEquals(Long.valueOf(44879160), sum.get());
        } finally {
            shutdownCluster(forkJoinPool);
        }
    }

    @Test
    public void testBasic_clustered_localSums() throws Exception {
        pools = createCluster(2, 1);
        TaskCachingDistributedForkJoinPool pool1 = pools[0];
        TaskCachingDistributedForkJoinPool pool2 = pools[1];
        assertHappyCluster(pool1, pool2);

        long start = System.currentTimeMillis();
        final int end = 10000;
        Future<Long> sum = pool1.submit(new LocalSumTest(1, end, 10));
        log.info("The sum of the numbers from 1 to " + end + " is " + sum.get() + " (and it took "
                + (System.currentTimeMillis() - start) + " ms to calculate)");
        assertEquals(Long.valueOf(44879160), sum.get());

        shutdownCluster(pool1, pool2);
        // of course, nothing was distributed; they weren't distributable jobs.
    }

    @Test
    public void testBasic_clustered_distSums_oneServer() throws Exception {
        pools = createCluster(1, 1);
        runClusteredTest();
    }

    @Test(timeOut = 100000)
    public void testBasic_clustered_distSums_twoServers() throws Exception {
        TestCacheManagerFactory.backgroundTestStarted(this);
        pools = createCluster(2, 1);
        runClusteredTest();
    }

    @Test(timeOut = 100000)
    public void testBasic_clustered_distSums_fiveServers() throws Exception {
        TestCacheManagerFactory.backgroundTestStarted(this);
        TaskCachingDistributedForkJoinPool[] pools = createCluster(5, 1);
        this.pools = pools;
        runClusteredTest();
    }

    @Test(timeOut = 100000)
    public void testBasic_clustered_distSums_cpuServers() throws Exception {
        TestCacheManagerFactory.backgroundTestStarted(this);
        TaskCachingDistributedForkJoinPool[] pools = createCluster(Runtime.getRuntime().availableProcessors(), 1);
        this.pools = pools;
        runClusteredTest();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        DistributedSumTask.taskMap.clear();
        DistributedSumTask.executionCount.set(0);
        if (pools != null) {
            shutdownCluster(pools);
        }
    }

    private void assertInnerMaps(TaskCachingDistributedForkJoinPool... pools) {
        for (TaskCachingDistributedForkJoinPool pool : pools) {
            assertEquals("taskIdToTaskMap should be empty: " + pool.taskIdToTaskMap, 0, pool.taskIdToTaskMap.size());
        }
    }

    private TaskCachingDistributedForkJoinPool[] createCluster(int numberOfMembers, int parallelism) {
        int newCacheManagersRequired = Math.max(0, numberOfMembers - cacheManagers.size());
        createClusteredCaches(newCacheManagersRequired, cacheName(), cb);

        TaskCachingDistributedForkJoinPool[] list = new TaskCachingDistributedForkJoinPool[numberOfMembers];
        for (int i = 0; i < numberOfMembers; i++) {
            list[i] = new TaskCachingDistributedForkJoinPool(cache(i, cacheName()), parallelism);
        }
        return list;
    }

    protected void runClusteredTest() throws InterruptedException, ExecutionException {
        if (pools.length > 1) {
            waitForClusterToForm(cacheName() + ".inf.fj.task-cache");
        }
        assertHappyCluster(pools);

        long start = System.currentTimeMillis();
        final int end = 10000;
        Future<Long> sum = pools[0].submit(new DistributedSumTask(1, end, 10, 10));
        log.info("The sum of the numbers from 1 to " + end + " is " + sum.get() + " (and it took "
                + (System.currentTimeMillis() - start) + " ms to calculate on " + pools.length + " node"
                + (pools.length > 1 ? "s" : "") + ")");
        assertEquals(Long.valueOf(44879160), sum.get());// the correct answer for 10,000. If you change this to 100,000, make it 4180778064L

        ByteArrayOutputStream byteout = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(byteout);
        printInterestingOutput(end, outStream, pools);
        log.info(byteout.toString());

        Map<Object, Long> originationCountMap = new HashMap<Object, Long>();
        Map<Object, Long> executionCountMap = new HashMap<Object, Long>();
        Map<Integer, Long> taskCountMap = new HashMap<Integer, Long>();
        buildTaskDistributionMaps(originationCountMap, executionCountMap, taskCountMap);
        assertEquals("expected every node to have executed at least one task. Execution map was: " + executionCountMap,
                pools.length, executionCountMap.size());
        assertEquals("expected every node to have originated at least one task. Origination map was: "
                + originationCountMap, pools.length, originationCountMap.size());

        log.info("Executed " + DistributedSumTask.executionCount + " tasks total");
        assertInnerMaps(pools);
    }

    protected void buildTaskDistributionMaps(Map<Object, Long> originationCountMap, Map<Object, Long> executionCountMap,
                                             Map<Integer, Long> taskCountMap) {
        for (int i = 0; i < pools.length; i++) {
            Collection<InfinispanDistributedForkJoinPool.InternalTask> tasks = pools[i].taskCache.values();
            long count = 0;
            for (InfinispanDistributedForkJoinPool.InternalTask internalTask : tasks) {
                addToMap(executionCountMap, internalTask.executedBy);
                addToMap(originationCountMap, internalTask.originAddress);
            }
            taskCountMap.put(i, count);
        }
    }

    private void addToMap(Map<Object, Long> map, Address a) {
        if (!map.containsKey(a)) {
            map.put(a, 1L);
        } else {
            map.put(a, map.get(a) + 1);
        }
    }

    private void shutdownCluster(TaskCachingDistributedForkJoinPool... pools) throws InterruptedException {
        log.info("Shutting down " + pools.length + " pools.");
        for (TaskCachingDistributedForkJoinPool pool : pools) {
            pool.shutdown();
            pool.shutdownNow();
        }
    }


    protected static void printInterestingOutput(int countTo, PrintStream out, TaskCachingDistributedForkJoinPool... pools) {
        out.println("\n\n**********************************************************");
        out.println("Executed " + DistributedSumTask.executionCount + " tasks to sum " + countTo + " numbers across "
                + pools.length + " servers");
        // note that the metrics are held by name, so all instances on the same JVM are sharing the same metric.
        // If we really wanted to track per-instance metrics, we'd need an "instance number" in the metric name or something
        // like that.
        //    	log.info("Jobs stolen: " + TaskCachingDistributedForkJoinPool.stolenJobsMeter.count());
        //    	log.info("Jobs stolen from: " + TaskCachingDistributedForkJoinPool.jobsStolenFromMeMeter.count());
        //    	log.info("distributable jobs count: " + TaskCachingDistributedForkJoinPool.distributableJobsMeter.count());
        //    	log.info("distributable jobs rate:  " + TaskCachingDistributedForkJoinPool.distributableJobsMeter.meanRate());
        //    	log.info("distributable jobs max:   " + TaskCachingDistributedForkJoinPool.distributableJobsHistogram.max());
        //    	log.info("bypassed queue:   " + TaskCachingDistributedForkJoinPool.bypassed.count());
        for (TaskCachingDistributedForkJoinPool pool : pools) {
            out.println("Pool " + pool.myAddress + ":");
            out.println("  Total distributable jobs: " + pool.distributableJobs.longValue());
            out.println("  Jobs that bypassed the 'distributable' mechanism: " + pool.distributableJobs.longValue());
            out.println("  Jobs that I originated and executed: " + pool.distributableExecutedAsOriginator.longValue());
            out.println("  Jobs that I executed but did not originate: " + pool.distributableExecutedNotOriginator.longValue());
        }

        // let's try calculating some statistics using our TaskMap, if present.
        if (DistributedSumTask.taskMap.size() > 0) {
            Map<String, Integer> finalTasksExecutedBy = getFinalTaskExecutors(DistributedSumTask.taskMap);
            Map<String, Integer> overallTasksExecutedBy = getTaskExecutors(DistributedSumTask.taskMap);
            double avgHopsPerTask = getAvgHopsPerTask(DistributedSumTask.taskMap);
            out.println("Average hops per task: " + avgHopsPerTask);
            out.println("Final tasks executed per node: " + sortMap(finalTasksExecutedBy));
            out.println("Overall tasks executed per node: " + sortMap(overallTasksExecutedBy));
        }
        out.println("\n\n**********************************************************");
        out.flush();
    }

    private static String sortMap(Map<String, Integer> map) {
        Set<String> keys = new TreeSet<String>(map.keySet());
        StringBuilder sb = new StringBuilder();
        for (String key : keys) {
            sb.append("\n").append(" - ").append(key).append(": ").append(map.get(key));
        }
        return sb.toString();
    }

    private static double getAvgHopsPerTask(ConcurrentMap<String, List<String>> taskMap) {
        double sum = 0;
        double count = 0;
        for (List<String> steps : taskMap.values()) {
            String currentStep = "new";
            for (String step : steps) {
                String name = getTaskName(step);
                if (!name.equals(currentStep)) {
                    sum++;
                    currentStep = name;
                }
            }
            count++;
        }
        return sum / count;
    }

    private static Map<String, Integer> getTaskExecutors(ConcurrentMap<String, List<String>> taskMap) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (List<String> values : taskMap.values()) {
            for (String value : values) {
                String taskName = getTaskName(value);
                increment(map, taskName);
            }
        }
        return map;
    }

    private static String getTaskName(String value) {
        Matcher m = Pattern.compile("ForkJoinPool-(\\d+)-.*").matcher(value);
        m.find();
        return m.group(1);
    }

    private static void increment(Map<String, Integer> map, String taskName) {
        int prev = 0;
        if (map.containsKey(taskName)) {
            prev = map.get(taskName);
        }
        map.put(taskName, prev + 1);
    }

    protected static Map<String, Integer> getFinalTaskExecutors(ConcurrentMap<String, List<String>> taskMap) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (List<String> values : taskMap.values()) {
            String taskName = getTaskName(values.get(values.size() - 1));
            increment(map, taskName);
        }
        return map;
    }

    private void assertHappyCluster(TaskCachingDistributedForkJoinPool... pools) throws InterruptedException {
        for (TaskCachingDistributedForkJoinPool pool : pools) {
            assertNotNull(pool);
        }

        for (TaskCachingDistributedForkJoinPool pool : pools) {
            // assert that they're clustered
            assertEquals("Wrong number of cluster members (this pool sees " + pool.cache.getRpcManager().getMembers()
                    + ")", pools.length, pool.cache.getRpcManager().getMembers().size());
            //         assertEquals(pools.length, pool.taskCache.getRpcManager().getMembers().size());
        }
    }

    private String getUniqueName() {
        return "q-test-" + UUID.randomUUID().toString();
    }

}
