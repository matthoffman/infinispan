package org.infinispan.distexec.fj;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.UUIDKeyGenerator;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.concurrent.jdk8backported.LongAdder;

import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * TODO: document me.
 *
 */
public abstract class InfinispanDistributedForkJoinPool extends AbstractDistributedForkJoinPool {
    /**
     * Used by our KeyAffinityService
     */
    protected static final int KEY_QUEUE_SIZE = 100;
    /**
     * The cache we're using to target tasks; if our tasks are processing cached data, then this will
     * be the cache that holds the data we're processing. If we're not, then we'll just use the cache
     * for its distribution configuration.
     */
    protected final AdvancedCache cache;
    /**
     * We use this to generate keys that are guaranteed to map to our local node. That way, when we
     * have a job submitted that doesn't have a cache key attached, we can still add it to the task
     * cache (so that it is distributed, and can be stolen) but we have the chance of working on it
     * locally. Which we would generally prefer.
     */
    final KeyAffinityService keyAffinityService;
    /**
     * We use extra threads for a number of small things
     */
    final ExecutorService utilityThreadPool = Executors.newCachedThreadPool();
    /**
     * note that we rely on this executorService's behavior: it will run things with a fixed delay,
     * and if one execution blocks (waiting for available work, for example) it will not concurrently
     * execute any additional executions.
     */
    final ScheduledExecutorService scheduledThreadPool;
    /**
     * we have to keep track of tasks that we've generated locally and then handed off to remote
     * nodes to execute, because we'll have other tasks within the ForkJoinPool that are waiting for
     * those tasks to complete (in an Object.wait() sense); when a remote node completes the task, we
     * need to mark our representation of that task complete.
     */
    final ConcurrentMap<Object, DistributedFJTask<?>> taskIdToTaskMap = CollectionFactory.makeConcurrentMap();
    /**
     * The address of this node.
     */
    final Address myAddress;
    /**
     * A few metrics we'll keep for reporting purposes
     */
    protected LongAdder distributableExecutedAsOriginator = new LongAdder();
    protected LongAdder distributableExecutedNotOriginator = new LongAdder(); // this is probably overkill; I don't really care if we miss a count now and then.
    protected Long unsuccessfulCheckout = 0L;

    public InfinispanDistributedForkJoinPool(int parallelism, ForkJoinWorkerThreadFactory factory, Thread.UncaughtExceptionHandler handler, boolean asyncMode, Cache cache) {
        super(parallelism, factory, handler, asyncMode);
        this.cache = cache.getAdvancedCache();
        scheduledThreadPool = Executors.newScheduledThreadPool(2);
        this.keyAffinityService = KeyAffinityServiceFactory.newKeyAffinityService(cache, utilityThreadPool,
                new UUIDKeyGenerator(), KEY_QUEUE_SIZE);
        this.myAddress = getOurAddress(cache);
    }

    public static Address getOurAddress(Cache cache) {
        RpcManager rpc = cache.getAdvancedCache().getRpcManager();
        if (rpc != null) {
            Address a = rpc.getAddress();
            if (a != null) {
                return a;
            }
        }
        return DefaultExecutorService.LOCAL_MODE_ADDRESS;
    }

    /**
     * This doesn't make any sense for this executor, at least how it's currently written; we don't use DistributedTasks. They're wrappers for Callables, and we need to wrap ForkJoinTasks.
     * @param callable the execution unit of DistributedTask
     * @param <T>
     * @return
     */
//    public <T> DistributedTaskBuilder<T> createDistributedTaskBuilder(Callable<T> callable) {
//        // TODO Auto-generated method stub
//        return null;
//    }


    /**
     * Our internal wrapper around a ForkJoinTask, tracking its state and anything else we need to
     * know when distributing it.
     *
     * Intentionally immutable; it makes reasoning about it easier.
     *
     * @author Matt Hoffman
     * @since 5.2
     */
    public static class InternalTask implements Serializable {
        private static final long serialVersionUID = -8205266104709317848L;

        enum Status {
            READY, IN_PROGRESS, COMPLETE, FAILED
        }

        final Object taskId;

        final TaskCachingDistributedForkJoinPool.InternalTask.Status status;

        final DistributedFJTask task;

        final Address originAddress;
        Address executedBy;

        final private Throwable exception;

        public InternalTask(Object taskId, DistributedFJTask forkJoinTask, Address originAddress, TaskCachingDistributedForkJoinPool.InternalTask.Status status,
                            Address currentlyWorkingOn, Throwable throwable) {
            this.taskId = taskId;
            this.task = forkJoinTask;
            this.status = status;
            this.originAddress = originAddress;
            this.executedBy = currentlyWorkingOn;
            this.exception = throwable;
        }

        public InternalTask(Object taskId, DistributedFJTask forkJoinTask, Address originAddress) {
            this(taskId, forkJoinTask, originAddress, TaskCachingDistributedForkJoinPool.InternalTask.Status.READY, null, null);
        }

        public DistributedFJTask getFJTask() {
            return task;
        }

        public TaskCachingDistributedForkJoinPool.InternalTask.Status getStatus() {
            return status;
        }

        public boolean isComplete() {
            return status == TaskCachingDistributedForkJoinPool.InternalTask.Status.FAILED || status == TaskCachingDistributedForkJoinPool.InternalTask.Status.COMPLETE;
        }

        public Address getOriginAddress() {
            return originAddress;
        }

        public Address getExecutedBy() {
            return executedBy;
        }

        public Throwable getThrowable() {
            return exception;
        }

        public Object getTaskId() {
            return taskId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((executedBy == null) ? 0 : executedBy.hashCode());
            result = prime * result + ((originAddress == null) ? 0 : originAddress.hashCode());
            result = prime * result + ((status == null) ? 0 : status.hashCode());
            result = prime * result + ((task == null) ? 0 : task.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            InternalTask other = (InternalTask) obj;
            if (executedBy == null) {
                if (other.executedBy != null)
                    return false;
            } else if (!executedBy.equals(other.executedBy))
                return false;
            if (originAddress == null) {
                if (other.originAddress != null)
                    return false;
            } else if (!originAddress.equals(other.originAddress))
                return false;
            if (status != other.status)
                return false;
            if (task == null) {
                if (other.task != null)
                    return false;
            } else if (!task.equals(other.task))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "InternalTask [" + (taskId != null ? "taskId=" + taskId + ", " : "")
                    + (status != null ? "status=" + status + ", " : "")
                    + (exception != null ? "exception=" + exception + ", " : "")
                    + (originAddress != null ? "created by=" + originAddress + ", " : "")
                    + (executedBy != null ? "executed by=" + executedBy + ", " : "") + (task != null ? "task=" + task : "")
                    + "]";
        }

    }
}
