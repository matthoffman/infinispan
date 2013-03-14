package org.infinispan.distexec.fj;

import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.util.concurrent.jdk8backported.ForkJoinPool;
import org.infinispan.util.concurrent.jdk8backported.ForkJoinTask;
import org.infinispan.util.concurrent.jdk8backported.LongAdder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.Thread.UncaughtExceptionHandler;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * Adds a distributed processing wrapper around a normal ForkJoinPool.
 *
 */
public abstract class AbstractDistributedForkJoinPool extends ForkJoinPool implements DistributedExecutorService {

    protected static final Log log = LogFactory.getLog(AbstractDistributedForkJoinPool.class);

    //	protected static final Meter bypassed = Metrics.defaultRegistry().newMeter(AbstractDistributedForkJoinPool.class, "bypassed-deque-meter", "job", TimeUnit.SECONDS);
    //    protected static final Meter distributableJobsMeter = Metrics.defaultRegistry().newMeter(AbstractDistributedForkJoinPool.class, "distributable-jobs-meter", "job", TimeUnit.SECONDS);

    /**
     * an estimate of whether our internal task queue is empty right now. This isn't guaranteed to be
     * accurate, but is a helpful marker so that we don't have to keep polling our internal queue.
     */
    protected volatile boolean workAvailable = true;

    protected boolean trackMetrics = true;

    protected LongAdder bypassed = new LongAdder();
    protected LongAdder distributableJobs = new LongAdder();

    protected final PoolNeedsWorkPolicy poolNeedsWorkPolicy;

    public AbstractDistributedForkJoinPool() {
        super();
        // repeating this in each constructor to avoid copying and pasting the defaults for other parameters from the superclass.
        this.poolNeedsWorkPolicy = new HasQueuedSubmissions();
    }

    public AbstractDistributedForkJoinPool(int parallelism) {
        super(parallelism);
        // repeating this in each constructor to avoid copying and pasting the defaults for other parameters from the superclass.
        this.poolNeedsWorkPolicy = new HasQueuedSubmissions();
    }

    public AbstractDistributedForkJoinPool(int parallelism, ForkJoinWorkerThreadFactory factory,
                                           UncaughtExceptionHandler handler, boolean asyncMode) {
        this(parallelism, factory, handler, asyncMode, new HasQueuedSubmissions());

    }

    public AbstractDistributedForkJoinPool(int parallelism, ForkJoinWorkerThreadFactory factory,
                                           UncaughtExceptionHandler handler, boolean asyncMode, PoolNeedsWorkPolicy poolNeedsWorkPolicy) {
        super(parallelism, factory, handler, asyncMode);
        this.poolNeedsWorkPolicy = poolNeedsWorkPolicy;
    }


    protected <T> ForkJoinTask<T> submitDistributable(ForkJoinTask<T> task) {
        //            if (ForkJoinTask.inForkJoinPool() && isQueueBackedUp(ForkJoinTask.getSurplusQueuedTaskCount())) {
        // mark that there is work available to be distributed?
        // do I want to do something here? Proactively try to find someone else to take this task?
        // I think no...this is a poll-only algorithm.
        //            }

        // we could get the number of queued submissions, and submit if the # is low enough.
        // however, we are trying very hard to minimize the impact on the underlying ForkJoinPool.
        // It's dealing with the finer-grained tasks, and is more sensitive to small latencies.
        // The "hasQueuedSubmissions" method has fairly low impact on the ForkJoinPool.

        // looking at the code, getQueuedSubmissionCount looks fairly inexpensive. More expensive, certainly,
        // if there are a lot of queues, because getQueuedSubmissionCount has to go through every queue, while
        // hasQueuedSubmissions just returns on the first one. But still...that might be worth it if we find our queue idling.
        if (!this.hasQueuedSubmissions()) {
            // TODO: do we need to check if this task's keys() line up with what
            // we have locally? (canExecuteLocally() returns true?)
            // if our queue is empty, but this task requires data that isn't on
            // this machine, we probably ought to distribute it to someone that
            // has the data anyway, right?

            // our queue is empty. Go ahead and send it on to the internal FJ
            // pool.
            bypassed.increment();
            return submitDirectly(task);
        } else {
            addWork(task);
            if (trackMetrics) {
                distributableJobs.increment();
            }
            return task;
        }
    }

    protected abstract <T> void addWork(ForkJoinTask<T> task);

    protected abstract <T> void notifyListenersOfNewTask(ForkJoinTask<T> task);


   	/*
     * intercept any submission method that takes a ForkJoinTask. if the task
	 * being distributed implements DistributableFJTask (or maybe we have some
	 * other way of flagging this task as "distributable", like a different
	 * entry point) then we hold onto it in a special "distributable" queue.
	 * otherwise, we stick it in the normal FJPool.
	 * 
	 * tasks move from the distributable queue to the normal queue at a regular
	 * interval, when the normal queue is empty (or close to empty). We may use
	 * this.hasQueuedSubmissions() for this, depending.
	 * 
	 * This isn't ideal; having the workqueue as a separate entity means that
	 * the queue could be starved for POLL_INTERVAL milliseconds before things
	 * from the distributable queue make it into the normal work queue. But
	 * without reworking the ForkJoinPool itself, there's not much we can do
	 * about that. And ForkJoinPool is highly optimized for what it does; the
	 * distributable tasks are by nature less time-sensitive (they're coarser
	 * grained, thus less sensitive to a few milliseconds of wait). So the
	 * inefficiencies here are hopefully bearable.
	 */
    // lots of room for optimization here, though.

    /**
     * Typically, task will be a subclass of either {@link LocalFJTask} or
     * {@link DistributedFJTask}. If task is a subclass of
     * {@link DistributedFJTask}, then the task could be handed off to another
     * node if it seems more efficient. If it is not a subclass of
     * DistributableTask, it is guaranteed to be processed here.
     * <p/>
     * tasks are idempotent, can we? Or can we? Not yet sure that I want to have
     * a timeout with each task submission. Does the caller necessarily know how
     * long a task will take to run? Should they be expected to?
     *
     * @param task
     * @param <T>
     * @return
     */
    @Override
    public <T> ForkJoinTask<T> submit(ForkJoinTask<T> task) {
        // increment our # of tasks.
        if (!workAvailable()) {
            notifyListenersOfNewTask(task);
        }

        if (task instanceof DistributedFJTask) {
            return submitDistributable(task);
        } else {
            return submitDirectly(task);
        }
    }

    @Override
    public <T> T invoke(ForkJoinTask<T> task) {
        if (task instanceof DistributedFJTask) {
            submitDistributable(task);
        } else {
            submitDirectly(task);
        }
        return task.join();
    }

    @Override
    public void execute(ForkJoinTask<?> task) {
        if (task instanceof DistributedFJTask) {
            submitDistributable(task);
        } else {
            submitDirectly(task);
        }
    }

    public <T> ForkJoinTask<T> submitDirectly(ForkJoinTask<T> task) {
        return super.submit(task);
    }

    boolean workAvailable() {
        return workAvailable;
    }

    /**
     * Returns true if the forkJoinPool could use additional work. This doesn't
     * necessarily mean that the pool is currently idle; only that we predict
     * that the pool will be idle soon. The accuracy of that prediction and the
     * definition of "soon" are intentionally left squishy, because they may
     * vary by implementation. The default implementation simply looks at
     * forkJoinPool.hasQueuedSubmissions(), which returns true if there are
     * tasks in the queue that have not yet been started. Therefore it can still
     * return true even though every thread is currently executing, provided
     * there is no additional work in the queue. That is the desired behavior,
     * on the assumption that a.) each task is fairly short-lived, and b.)
     * fetching more work may involve a distributed operation, which could take
     * some time. Therefore, it's better to start fetching more work before any
     * threads are idle, on the hopes that it will be ready for them when they
     * need it.
     *
     * However, implementors may want to override this behavior. If, on the one
     * hand, fetching more work takes a particularly long time, you may want to
     * replace this with a heuristic that fetches more work earlier -- perhaps
     * when the work queue reaches a particular threshold. On the other hand, if
     * tasks are particularly short-lived, fetching tasks is very fast, and your
     * application is particularly latency-sensitive, you may prefer to fetch
     * work only when threads are actually idling.
     *
     * @return
     */
    protected boolean poolNeedsWork() {
        return poolNeedsWorkPolicy.needsWork(this);
    }

    public static interface PoolNeedsWorkPolicy {

        /**
         * Returns true if the given pool needs work and should look for potentially-distributable work to execute.
         *
         * @param forkJoinPool a distributed fork-join pool
         * @return
         */
        boolean needsWork(AbstractDistributedForkJoinPool forkJoinPool);

    }

    protected static class HasQueuedSubmissions implements PoolNeedsWorkPolicy {

        @Override
        public boolean needsWork(AbstractDistributedForkJoinPool forkJoinPool) {
            return !forkJoinPool.hasQueuedSubmissions();
        }
    }

}