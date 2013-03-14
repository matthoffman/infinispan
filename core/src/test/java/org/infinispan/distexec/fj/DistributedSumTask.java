package org.infinispan.distexec.fj;

import org.infinispan.util.concurrent.jdk8backported.ConcurrentHashMapV8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Very similar to the local version, but creates child DistributedSumTest
 * instances instead of LocalSumTests.
 */
public class DistributedSumTask extends DistributedFJTask<Long> {
    static final Logger log = LoggerFactory.getLogger(DistributedSumTask.class);

    private static final long serialVersionUID = 1508664573598369190L;

    public static final ConcurrentMap<String, List<String>> taskMap = new ConcurrentHashMapV8<String, List<String>>();

    final int start;
    final int end;
    final int threshold;
    final int wait;
    long executedOn;
    String executedBy;
    List<String> parentsExecutedBy = new ArrayList<String>();
    public static AtomicLong executionCount = new AtomicLong(0);

    public DistributedSumTask(int start, int end, int threshold, int wait) {
        this.start = start;
        this.end = end;
        this.threshold = threshold;
        this.wait = wait;
    }

    @Override
    protected Long doCompute() {
        executionCount.incrementAndGet();
        if (executedOn != 0) {
            log.error("task already executed!");
        }
        executedOn = System.currentTimeMillis();
        executedBy = Thread.currentThread().getName();
        if (wait > 0) {
            busywait(wait);
        }
        List<String> lineage = calcLineage();
        if ((end - start) > threshold) {
            // split the job in two
            int newEnd = start + (end - start) / 2;// pick a point halfway
            // between start and end.

            DistributedSumTask job1 = new DistributedSumTask(start, newEnd, threshold, wait);
            job1.parentsExecutedBy = lineage;
            DistributedSumTask job2 = new DistributedSumTask(newEnd + 1, end, threshold, wait);
            job2.parentsExecutedBy = lineage;
            // submit those two
            invokeAll(job1, job2);
            // return the sum
            return job1.join() + job2.join();
        } else {
            recordLineage(lineage);
            // do the work
            long sum = 0;
            for (int i = start; i < end; i++) {
                sum += i;
            }
            return sum;
        }
    }

    private List<String> calcLineage() {
        List<String> lineage = new ArrayList<String>(this.parentsExecutedBy);
        lineage.add(executedBy); // add ourselves to the list.
        return lineage;
    }

    private void recordLineage(List<String> lineage) {
        // log.debug("Calculating sum of "+ start+" to "+ end+". Lineage is: "+
        // lineage);
        String key = start + "-" + end;
        // I don't like that this introduces a bottleneck that isn't inherent to
        // FJ, but it's for debugging purposes.
        // For performance-testing purposes, remove this.
        // List<String> prev = taskMap.putIfAbsent(key, lineage);
        // if (prev != null) {
        // log.error("**** Previously executed " + key + "! It had parents " +
        // prev);
        // }
    }

    private void busywait(int wait) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < (start + wait)) {
            // no really, just spin.
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("DistributedSumTest");
        sb.append("{").append(start);
        sb.append(" to ").append(end);
        sb.append('}');
        return sb.toString();
    }

}