package org.infinispan.distexec.fj;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;

/**
 * TODO: document me.
 *
 */
public class MultipleCacheDistributedForkJoinPoolTest extends MultipleCacheManagersTest {
    protected boolean supportsConcurrentUpdates = true;
    ConfigurationBuilder cb;

    @Override
    protected void createCacheManagers() throws Throwable {
        cb = createConfigurationBuilder();

    }

    protected ConfigurationBuilder createConfigurationBuilder() {
        ConfigurationBuilder configBuilder = getDefaultClusteredCacheConfig(getCacheMode(), false);
        configBuilder.locking().supportsConcurrentUpdates(supportsConcurrentUpdates);
        return configBuilder;
    }

    protected CacheMode getCacheMode() {
        return CacheMode.DIST_SYNC;
    }

    protected String cacheName() {
        return "DistributedFJPoolTest-DIST_SYNC";
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
