package org.infinispan.distexec.fj;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.concurrent.jdk8backported.ForkJoinPool;
import org.jgroups.Address;
import org.jgroups.util.Util;
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

import static junit.framework.Assert.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 *
 *
 */
@Test(groups = "functional", testName = "distexec.fj.DistributedForkJoinPoolTest")
public class JGroupsDistributedForkJoinPoolTest extends MultipleCacheDistributedForkJoinPoolTest {
//    static final Logger log = LoggerFactory.getLogger(DistributedForkJoinPoolTest.class);

    JGroupsDistributedForkJoinPool[] pools;

    public JGroupsDistributedForkJoinPoolTest() {
        cleanup = CleanupPhase.AFTER_METHOD;
    }

    protected String cacheName() {
        return "JGroupsDistributedForkJoinPoolTest-DIST_SYNC";
    }

    @AfterTest
    public void shutdownCache() {
        if (pools != null) {
            for (JGroupsDistributedForkJoinPool pool : pools) {
                pool.shutdown();
            }
        }
    }


    @Test
    public void sanityTest() throws Exception {
        // this isn't even testing our DistributedForkJoinPool; it's using a vanilla ForkJoinPool in order to test our test.
        ForkJoinPool plainForkJoinPool = new ForkJoinPool();
        long start = System.currentTimeMillis();
        final int end = 10000;
        Future<Long> sum = plainForkJoinPool.submit(new NormalFJSumTest(1, end, 10));
        log.info("The sum of the numbers from 1 to " + end + " is " + sum.get() + " (and it took " + (System.currentTimeMillis() - start) + " ms to calculate using a normal FJ pool)");
        assertEquals(Long.valueOf(44879160), sum.get());
    }

    @Test
    public void testBasic() throws Exception {
        createClusteredCaches(1, cacheName(), cb);
        for (EmbeddedCacheManager cacheManager : cacheManagers) {
            cacheManager.defineConfiguration(TaskCachingDistributedForkJoinPool.defaultTaskCacheConfigurationName,
                    cb.build());
        }
        JGroupsDistributedForkJoinPool forkJoinPool = null;
        try {
            forkJoinPool = new JGroupsDistributedForkJoinPool(cache(0, cacheName()), 1, null);
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
        String uniqueName = getUniqueName();
        pools = createCluster(2, uniqueName, 1);
        assertHappyCluster(pools[0], pools[1]);


        long start = System.currentTimeMillis();
        final int end = 10000;
        Future<Long> sum = pools[0].submit(new LocalSumTest(1, end, 10));
        log.info("The sum of the numbers from 1 to " + end + " is " + sum.get() + " (and it took " + (System.currentTimeMillis() - start) + " ms to calculate)");
        assertEquals(Long.valueOf(44879160), sum.get());

        shutdownCluster(pools[0], pools[1]);
        // of course, nothing was distributed; they weren't distributable jobs.
    }

    @Test
    public void testBasic_clustered_distSums_oneServer() throws Exception {
        String uniqueName = getUniqueName();
        pools = createCluster(1, uniqueName, 5);
        runClusteredTest();
    }

    @Test
    public void testBasic_clustered_distSums_twoServers() throws Exception {
        String uniqueName = getUniqueName();
        pools = createCluster(2, uniqueName, 1);
        runClusteredTest();
    }

    @Test
    public void testBasic_clustered_distSums_fiveServers() throws Exception {
        String uniqueName = getUniqueName();
        JGroupsDistributedForkJoinPool[] pools = createCluster(5, uniqueName, 1);
        this.pools = pools;
        runClusteredTest();
    }


    @Test
    public void testBasic_clustered_distSums_cpuServers() throws Exception {
        String uniqueName = getUniqueName();
        JGroupsDistributedForkJoinPool[] pools = createCluster(Runtime.getRuntime().availableProcessors(), uniqueName, 1);
        this.pools = pools;
        runClusteredTest();
    }


    @Test
    public void testSerializeMessages() throws Exception {
        DistributedSumTask task = new DistributedSumTask(1, 100, 10, 0);
        JGroupsDistributedForkJoinPool.ExecuteMessage response = new JGroupsDistributedForkJoinPool.ExecuteMessage(task, Collections.<Address>emptyList());
        byte[] bytes = Util.objectToByteBuffer(response);
        Object o = Util.objectFromByteBuffer(bytes);
        assertNotNull(o);
        assertTrue(o instanceof JGroupsDistributedForkJoinPool.ExecuteMessage);
        assertEquals(task, ((JGroupsDistributedForkJoinPool.ExecuteMessage) o).getTask());
    }

    @AfterTest
    public void tearDown() throws Exception {
        DistributedSumTask.taskMap.clear();
        DistributedSumTask.executionCount.set(0);
        if (pools != null) {
            shutdownCluster(pools);
        }
    }

    private void assertInnerMaps(JGroupsDistributedForkJoinPool... pools) {
        for (JGroupsDistributedForkJoinPool pool : pools) {
            assertEquals("taskIdToTaskMap should be empty: " + pool.taskIdToTaskMap, 0, pool.taskIdToTaskMap.size());
            //TODO: assert correct inner maps
//            for (Set<String> taskSets : pool.nodeToTaskMap.values()) {
//                the map may have some entries in it, but they should all be node Ids to empty sets. That's ok.
//                assertEquals("nodeToTaskMap should be empty: " + pool.nodeToTaskMap, 0, taskSets.size());
//            }
        }
    }


    private JGroupsDistributedForkJoinPool[] createCluster(int numberOfMembers, String groupName, int parallelism) {
        int newCacheManagersRequired = Math.max(0, numberOfMembers - cacheManagers.size());
        createClusteredCaches(newCacheManagersRequired, cacheName(), cb);

        JGroupsDistributedForkJoinPool[] list = new JGroupsDistributedForkJoinPool[numberOfMembers];
        for (int i = 0; i < numberOfMembers; i++) {
            list[i] = new JGroupsDistributedForkJoinPool(cache(i, cacheName()), parallelism, null);
        }
        return list;
    }


    protected void runClusteredTest() throws InterruptedException, ExecutionException {
        assertHappyCluster(pools);

        long start = System.currentTimeMillis();
        final int end = 10000;
        Future<Long> sum = pools[0].submit(new DistributedSumTask(1, end, 10, 10));
        log.info("The sum of the numbers from 1 to " + end + " is " + sum.get() + " (and it took " + (System.currentTimeMillis() - start) + " ms to calculate on " + pools.length + " node" + (pools.length > 1 ? "s" : "") + ")");
        assertEquals(Long.valueOf(44879160), sum.get());// the correct answer for 10,000. If you change this to 100,000, make it 4180778064L

        ByteArrayOutputStream byteout = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(byteout);
        printInterestingOutput(end, outStream, pools);
        log.info(byteout.toString());

//        if (pools.length > 1) {
//            assertTrue("expected something to be stolen from pool1", pools[0].jobsStolenFromMeMeter.get() > 0);
//        }

        log.info("Executed " + DistributedSumTask.executionCount + " tasks total");
        assertInnerMaps(pools);
    }

    private void shutdownCluster(JGroupsDistributedForkJoinPool... pools) throws InterruptedException {
        log.info("Shutting down " + pools.length + " pools.");
        for (JGroupsDistributedForkJoinPool pool : pools) {
            if (pool != null) {
                pool.shutdown();
                pool.shutdownNow();
            }
        }
    }

    protected static void printInterestingOutput(int end, PrintStream out, JGroupsDistributedForkJoinPool... pools) {
        out.println("\n\n**********************************************************");
        out.println("** Note that metrics are aggregated from previous tests.** ");
        out.println("Executed " + DistributedSumTask.executionCount + " tasks to sum " + end + " numbers across "
                + pools.length + " servers");
        // note that the metrics are held by name, so all instances on the same JVM are sharing the same metric.
        // If we really wanted to track per-instance metrics, we'd need an "instance number" in the metric name or something
        // like that.
        //    	out.println("Jobs stolen: " + DefaultDistributedForkJoinPool.stolenJobsMeter.count());
        //    	out.println("Jobs stolen from: " + DefaultDistributedForkJoinPool.jobsStolenFromMeMeter.count());
        //    	out.println("distributable jobs count: " + DefaultDistributedForkJoinPool.distributableJobsMeter.count());
        //    	out.println("distributable jobs rate:  " + DefaultDistributedForkJoinPool.distributableJobsMeter.meanRate());
        //    	out.println("distributable jobs max:   " + DefaultDistributedForkJoinPool.distributableJobsHistogram.max());
        //    	out.println("bypassed queue:   " + DefaultDistributedForkJoinPool.bypassed.count());

        // let's try calculating some statistics using our TaskMap, if present.
        if (DistributedSumTask.taskMap.size() > 0) {
            Map<String, Integer> finalTasksExecutedBy = getFinalTaskExecutors(DistributedSumTask.taskMap);
            Map<String, Integer> overallTasksExecutedBy = getTaskExecutors(DistributedSumTask.taskMap);
            double avgHopsPerTask = getAvgHopsPerTask(DistributedSumTask.taskMap);
            out.println("Average hops per task: " + avgHopsPerTask);
            out.println("Final tasks executed per node: " + sortMap(finalTasksExecutedBy));
        }
        out.println("\n\n**********************************************************");
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

    private static Map<String, Integer> getFinalTaskExecutors(ConcurrentMap<String, List<String>> taskMap) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (List<String> values : taskMap.values()) {
            String taskName = getTaskName(values.get(values.size() - 1));
            increment(map, taskName);
        }
        return map;
    }


    private void assertHappyCluster(JGroupsDistributedForkJoinPool... pools) throws InterruptedException {
        for (JGroupsDistributedForkJoinPool pool : pools) {
            assertNotNull(pool);
        }

        // give them a moment to meet and shake hands.
        Thread.sleep(100);

        for (JGroupsDistributedForkJoinPool pool : pools) {
            // assert that they're clustered
            assertEquals(pools.length, pool.getView().getMembers().size());
        }
    }


    private String getUniqueName() {
        return "q-test-" + UUID.randomUUID().toString();
    }


    public static class LocalSumTest extends LocalFJTask<Long> {
        private static final long serialVersionUID = -202940051703769607L;

        final int start;
        final int end;
        final int threshold;

        public LocalSumTest(int start, int end, int threshold) {
            this.start = start;
            this.end = end;
            this.threshold = threshold;
        }

        @Override
        protected Long compute() {
            if ((end - start) > threshold) {
                // split the job in two
                int newEnd = start + (end - start) / 2;// pick a point halfway between start and end.
                LocalSumTest job1 = new LocalSumTest(start, newEnd, threshold);
                LocalSumTest job2 = new LocalSumTest(newEnd + 1, end, threshold);
                // submit those two
                invokeAll(job1, job2);
                // return the sum
                return job1.join() + job2.join();
            } else {
                // do the work
                long sum = 0;
                for (int i = start; i < end; i++) {
                    sum += i;
                }
                return sum;
            }
        }
    }


    public static class NormalFJSumTest extends org.infinispan.util.concurrent.jdk8backported.RecursiveTask<Long> {
        final int start;
        final int end;
        final int threshold;

        public NormalFJSumTest(int start, int end, int threshold) {
            this.start = start;
            this.end = end;
            this.threshold = threshold;
        }

        @Override
        protected Long compute() {
            if ((end - start) > threshold) {
                // split the job in two
                int newEnd = start + (end - start) / 2;// pick a point halfway between start and end.
                NormalFJSumTest job1 = new NormalFJSumTest(start, newEnd, threshold);
                NormalFJSumTest job2 = new NormalFJSumTest(newEnd + 1, end, threshold);
                // submit those two
                invokeAll(job1, job2);
                // return the sum
                return job1.join() + job2.join();
            } else {
                // do the work
                long sum = 0;
                for (int i = start; i < end; i++) {
                    sum += i;
                }
                return sum;
            }
        }
    }
}
