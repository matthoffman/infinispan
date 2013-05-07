package org.infinispan.distexec.fj;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.concurrent.FutureListener;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.concurrent.NotifyingFutureAdaptor;
import org.infinispan.util.concurrent.jdk8backported.ForkJoinTask;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.*;

import static org.infinispan.util.Util.rewrapAsCacheException;

/**
 *
 * cache-based algorithm:
 *
 * <ol>
 *     <li>create a task cache, using the hashing configuration of the cache passed in on construction.</li>
 *     <li>add a listener to the cache that watches for new and updated entries</li>
 *     <li>tell other members of this cluster to create a distributed fork-join pool, too</li>
 *     <li>When a new task is added to the cache,</li>
 *     <ul>
 *         <li>if it's in our partition of the cache, add it to our internal 'available work' queue</li>
 *     </ul>
 *     <li>Meanwhile, poll our internal fork-join queue. If we need work (where "needs work" is pluggable), we'll pull
 *     a task off the 'available work' queue and attempt to check it out. </li>
 *     <li>If any of our tasks create a new task, and those are instances of DistributableFJTask,
 *     <ul>
 *         <li>if our pool already needs work, we'll go ahead and add it locally. No need to distribute it if we can
 *         handle it locally.</li>
 *         <li>if our pool already has work, we'll add it to the task cache. It will automatically be distributed to
 *         other members and the first available member will execute it.</li>
 *     </ul>
 * </ol>
 *
 * Future enhancements:
 * <ul>
 *     <li>Do the proper internationalized logging stuff</li>
 *     <li>Implement the rest of the DistributedExecutor's methods (might need some thinking about how to make them make sense)</li>
 *     <li>Add something like "setEnvironment" on a DistributedFJTask</li>
 *     <li>Honor timeout and DistributedTaskFailover from DistributedTask</li>
 *     <li>define "churn"</li>
 *     <li>profile it!</li>
 *     <li>fix thread leakage in FJ pool (why so much blocking?)</li>
 * </ul>
 * Other enhancements to consider:
 *
 *
 * @author Matt Hoffman
 * @since 5.3
 *
 */
public class TaskCachingDistributedForkJoinPool extends InfinispanDistributedForkJoinPool {

    /**
     * The suffix we'll use to generate the name of the cache we're using to hold tasks.
     */
    protected String taskCacheNameSuffix = "inf.fj.task-cache";
    /**
     * We'll look for a cache with this name to use as the default template for our task cache. If it is not there,
     * we'll fall back to a hard-coded default.
     */
    public static final String defaultTaskCacheConfigurationName = "task-cache";

    /**
     * The actual cache we're using to hold tasks. This ought to be replicated. It is created in the constructor, based
     * on a combination of the default task cache configuration (or a default) along with the grouping and consistent
     * hashing configuration of the data cache passed in in the constructor.
     */
    protected final AdvancedCache<Object, InternalTask> taskCache;

    /**
     * Are we currently shutdown?
     */
    volatile boolean shutdown = false;

    /**
     * we'll use this as a queue of potentially available task ids. This is a
     * communication channel between our cache listener that is watching for
     * potentially executable tasks and our fork-join pool watcher, which is
     * looking to put more work in the pool when it's getting low.
     *
     * Note: if we want to support priority amongst tasks, this could be a
     * priority queue. But we'd need to implement a FIFOEntry wrapper class, as
     * described in the {@link java.util.concurrent.PriorityBlockingQueue}
     * javadoc, to preserve FIFO semantics amongst tasks of similar priority.
     *
     * Or, consider making this a deque, and putting local tasks on the front.
     * But a bit difficult to work out how that would work with priority
     */
    private final BlockingQueue<Object> availableTasks;

    /**
     * this is a communication channel between the fork-join tasks themselves and the thread that
     * registers them as "successful" or "failed" in the shared task cache. We use this so that the
     * ForkJoin infrastructure itself doesn't have to block while waiting for the cache to sync.
     */
    private final BlockingQueue<InternalTask> completedTaskQueue;

    /**
     * Create a new cache, using the provided cache as a template for data distribution.
     * @param cache
     */
    public TaskCachingDistributedForkJoinPool(Cache cache) {
        this(cache, Runtime.getRuntime().availableProcessors());
    }

    public TaskCachingDistributedForkJoinPool(Cache cache, int parallelism) {
        super(parallelism, defaultForkJoinWorkerThreadFactory, null /* TODO: Consider an uncaught exception handler? */, false, cache);

        //TODO: at this point, "cache" could be a local-mode cache. Does that really make any sense? Should we fail in that case?
        // we'll make the taskCache distributed, either way.
        this.taskCache = createTaskCache(cache, createTaskCacheName(cache.getName(), taskCacheNameSuffix));


        this.availableTasks = new LinkedBlockingQueue<Object>();

        this.completedTaskQueue = new LinkedBlockingQueue<InternalTask>();
        //      this.taskResultWatcher = new TaskResultWatcher(completedTaskQueue);

        this.taskCache.addListener(new AvailableTaskListener(taskCache));

        try {
            // start listening.
            // Normal Executors have no separate "start" method; they start on construction. So we have to do the same.
            start();
        } catch (Exception e) {
            log.error("Error creating distributed executor using cache '" + cache.getName() + "': ", e);
            throw rewrapAsCacheException(e);
        }
    }

    private String createTaskCacheName(String cacheName, String taskCacheName) {
        return cacheName + "." + taskCacheName;
    }

    @Override
    public void shutdown() {
        stop();
        super.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        // TODO: return tasks from the task cache?
        stop();
        return super.shutdownNow();
    }

    /**
     * Disconnect from the cluster. Once "stop" has been called, this server can no longer receive
     * work.
     */
    protected void stop() {
        shutdown = true;

        try {
            utilityThreadPool.shutdownNow();
        } catch (Throwable t) {
            log.warn("error while shutting down thread pool; will attempt to continue: ", t);
        }
        try {
            if (scheduledThreadPool != null) {
                scheduledThreadPool.shutdownNow();
            }
        } catch (Throwable t) {
            log.warn("error while shutting down thread pool; will attempt to continue: ", t);
        }
        try {
            taskCache.stop();
        } catch (Throwable t) {
            log.warn("error while shutting down task cache; will attempt to continue: ", t);
        }
    }

    /**
     * TODO: this method currently looks up the task cache by name; really, we need to create a new
     * one for each cache.
     *
     * @param templateCache this is the cache we'll use as a "template" for things like grouping, in the hopes that the taskCache we create is partitioned in exactly the same way.
     * @param cacheName the name to use for our task cache.
     * @return
     */
    protected AdvancedCache<Object, InternalTask> createTaskCache(Cache templateCache, String cacheName) {
        EmbeddedCacheManager cm = templateCache.getCacheManager();
        Configuration taskCacheConfiguration = cm.getCacheConfiguration(defaultTaskCacheConfigurationName);
        if (taskCacheConfiguration != null) {
            log.debug("Creating task cache " + cacheName + " using default task cache configuration '" + defaultTaskCacheConfigurationName + "'as a base.");
            taskCacheConfiguration = modifyConfiguration(taskCacheConfiguration, templateCache);
        } else {
            log.debug("Creating task cache " + cacheName + " based on defaults. To override these defaults, define a cache named '" + defaultTaskCacheConfigurationName + "'");
            taskCacheConfiguration = getDefaultCacheConfiguration(templateCache).build();
        }
        cm.defineConfiguration(cacheName, taskCacheConfiguration);
        Cache<Object, InternalTask> c = cm.getCache(cacheName, true);
        assertTaskCacheIsValid(c);
        return c.getAdvancedCache();
    }

    private Configuration modifyConfiguration(Configuration taskCacheConfiguration, Cache templateCache) {
        Configuration templateConfig = templateCache.getCacheConfiguration();
        ConfigurationBuilder cb = new ConfigurationBuilder().read(taskCacheConfiguration);

        // we always want to use the template cache's hash configuration, since we want to distribute data to the same
        // places that the template does.
        cb.clustering().hash().read(templateConfig.clustering().hash());
        return cb.build(true);
    }

    protected static ConfigurationBuilder getDefaultCacheConfiguration(Cache cache) {
        ConfigurationBuilder cb = new ConfigurationBuilder().read(cache.getCacheManager().getDefaultCacheConfiguration());
        cb.clustering().cacheMode(CacheMode.DIST_SYNC).sync().transaction().syncCommitPhase(true).syncRollbackPhase(true)
                .useSynchronization(true);
        Configuration existingCacheConf = cache.getCacheConfiguration();

        // inherit the template cache's hash configuration
        cb.clustering().hash().read(existingCacheConf.clustering().hash());

        // inherit JMX configuration
        cb.jmxStatistics().read(existingCacheConf.jmxStatistics());

        // inherit Sites configuration
        cb.sites().read(existingCacheConf.sites());

        return cb;
    }

    private void assertTaskCacheIsValid(Cache<Object, InternalTask> cache) {
        if (!cache.getCacheConfiguration().clustering().cacheMode().isDistributed()
                && !cache.getCacheConfiguration().clustering().cacheMode().isReplicated()) {
            throw new IllegalArgumentException("Task cache '" + cache.getName()
                    + "' is not distributed or replicated. This doesn't make sense for a task cache");
        }
    }

    public void start() {
        // this thread will just sit and run forever.
        utilityThreadPool.submit(new CompletionQueueWatcher(completedTaskQueue));

        scheduledThreadPool.scheduleAtFixedRate(new DistributedForkJoinQueueWatcher(this), 10, 10, TimeUnit.MILLISECONDS);
//      scheduledThreadPool.scheduleAtFixedRate(taskResultWatcher, 10, 5, TimeUnit.MILLISECONDS);

        //TODO: start up an executor on the other nodes, too?
//      cache.getRpcManager().broadcastRpcCommand( /* command to tell the remote machine to start up a cache */ );
    }

    @Override
    protected <T> void addWork(DistributedFJTask<T> forkJoinTask) {
        // is this task cache-aware?
        Object taskId;
        if (forkJoinTask instanceof KeyAwareDistributedFJTask<?>) {
            List<Object> keys = ((KeyAwareDistributedFJTask<?>) forkJoinTask).keys();
            // if so, add it to the cache using its desired cache key. If this task conforms to multiple,
            // we'll just pick the first. I'm not sure that a more intelligent algorithm would be worth
            // the cost here.
            taskId = keyAffinityService.getCollocatedKey(keys.get(0));
        } else {
            // if not, generate a key meaning "us" and add it to the cache.
            taskId = keyAffinityService.getKeyForAddress(myAddress);
        }
        InternalTask internalTask = new InternalTask(taskId, forkJoinTask, myAddress);
        // TODO: If this task is local to us, would we like to pop this onto the
        // front of the availableWorkQueue as well? So we're more likely to grab
        // it first?
        // TODO: this means
        taskCache.withFlags(Flag.IGNORE_RETURN_VALUES).putAsync(taskId, internalTask);
        taskIdToTaskMap.put(taskId, internalTask.getFJTask());

    }

    @Override
    protected <T> void notifyListenersOfNewTask(ForkJoinTask<T> task) {
        // TODO Auto-generated method stub

    }

    /**
     * This is the thread that watches the inner ForkJoinPool, tries to detect when it's empty (or
     * almost empty), and pulls a task from our pool of distributable tasks to give it.
     */
    protected class DistributedForkJoinQueueWatcher implements Runnable {
        final TaskCachingDistributedForkJoinPool forkJoinPool;

        public DistributedForkJoinQueueWatcher(TaskCachingDistributedForkJoinPool forkJoinPool) {
            this.forkJoinPool = forkJoinPool;
        }

        @Override
        public void run() {
            try {
                if (poolNeedsWork()) {
                    //               NotifyingFuture<InternalTask> futureTask = getWork();
                    //               if (futureTask != null) {
                    try {
                        //                     InternalTask task = futureTask.get();
                        InternalTask task = getWorkBlocking();
                        if (task != null) {
                            if (trackMetrics) {
                                if (myAddress.equals(task.getOriginAddress())) {
                                    distributableExecutedAsOriginator.increment();
                                } else {
                                    distributableExecutedNotOriginator.increment();
                                }
                            }
                            queueWork(task.getFJTask());
                        }
                    } catch (CancellationException e) {
                        // TODO: handle exception
                    } catch (InterruptedException e) {
                        // ok, we were interrupted. Let's be polite, then, and exit.
                        // clear the interrupted state first; it has been handled.
                        Thread.interrupted();
                    }
                }
                //            }
            } catch (Throwable t) {
                if (t instanceof InterruptedException
                        || (t instanceof ExecutionException && ((ExecutionException) t).getCause() instanceof InterruptedException)) {
                    log.trace("Thread interrupted; revisting in the next poll interval (but we're probably being shut down)");
                } else {
                    // don't let the thread die. Log an error and move on with life.
                    log.error(
                            "Got within the ForkJoinQueueWatcher. Exiting, and revisiting on the next poll interval. We have "
                                    + ((ThreadPoolExecutor) utilityThreadPool).getActiveCount() + " threads currently running, "
                                    + "and we've completed " + ((ThreadPoolExecutor) utilityThreadPool).getCompletedTaskCount()
                                    + " tasks", t);
                }
            }
        }

        public void queueWork(ForkJoinTask<?> forkJoinTask) {
            // submit it to the ForkJoinPool
            forkJoinPool.submitDirectly(forkJoinTask);
        }

    }

    /**
     * This spawns a new thread and returns a future. This is a debatable decision; the current
     * algorithm is actually just blocking on the future anyway, so we could make this a blocking
     * call. I envisioned this as a more generic interface, perhaps part of a transport-agnostic work
     * distribution algorithm.
     *
     * TODO: this would be more efficiently handled as a callback-based algorithm ('let me know when
     * there's work available'), rather than tying up a thread to poll the queue. Thankfully, there
     * should be only one thread per pool.
     *
     * @return a future that will either return a task, or a
     */
    NotifyingFuture<InternalTask> getWork() {
        // is there work available in my local partition of the task cache?
        //    If so, try to grab that.
        final NotifyingFutureAdaptor<InternalTask> result = new NotifyingFutureAdaptor<InternalTask>();
        try {
            Future<InternalTask> returnValue = utilityThreadPool.submit(new Callable<InternalTask>() {
                @Override
                public InternalTask call() throws Exception {
                    return getWorkBlocking();
                }

            });
            result.setActual(returnValue);
            return result;
        } catch (Error e) {
            log.error(
                    "Got an error while spawning a thread. We have "
                            + ((ThreadPoolExecutor) utilityThreadPool).getActiveCount() + " threads currently running, "
                            + "and we've completed " + ((ThreadPoolExecutor) utilityThreadPool).getCompletedTaskCount()
                            + " tasks", e);
            return null;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected InternalTask getWorkBlocking() throws InterruptedException {
        while (true) {
            Object taskId = availableTasks.take(); // blocks until something is available.
            if (taskId != null) {
                InternalTask task = taskCache.get(taskId);
                if (task == null) {
                    log.error("task ID '" + taskId + "' is in our availableTasks queue, but not available in the task map.");
                } else if (task.getStatus() == InternalTask.Status.READY) {
                    // I'd kind of like to check again whether we still need work. But we can't do it here, or this task will just drop on the floor.
                    //
                    // ok, now we need to claim it for ourselves.
                    // we'll use an atomic replace here, trusting that the STATUS is part of the equals() clause of InternalTask,
                    // and the distributed cache will atomically replace that with a IN_PROGRESS version if no one else has gotten to it first.
                    final InternalTask newTask = new InternalTask(taskId, task.getFJTask(), task.getOriginAddress(),
                            InternalTask.Status.IN_PROGRESS, myAddress, null);
                    boolean successful = taskCache.replace(taskId, task, newTask);
                    if (successful) {

                        //TODO: we have two potential ways of watching for task completion here. We can either put a listener on the task itself
                        // (which requires a bit of modification to the FJ classes to support) or we can just spawn a watcher thread and poll
                        // the tasks. I'm a bit undecided. The listener is cleaner, but the polling is simple and unobtrusive.
                        task.getFJTask().attachListener(new FutureListener() {

                            @Override
                            public void futureDone(Future future) {
                                completedTaskQueue.add(newTask);
                            }
                        });

                        // tell our taskResultWatcher to watch this task.
                        //                  taskResultWatcher.addTask(newTask);
                        return newTask;
                    } else {
                        // if not successful, then someone else has already checked it out. Oh well. Loop around and try the next one.
                        unsuccessfulCheckout++;
                    }
                }
            }
        }
    }

    protected void markTaskComplete(InternalTask task) {
        InternalTask newTask = new InternalTask(task.getTaskId(), task.getFJTask(), task.getOriginAddress(),
                InternalTask.Status.COMPLETE, myAddress, null);
        markSuccessOrFailureInCache(task, newTask);
    }

    private void markSuccessOrFailureInCache(InternalTask oldTask, InternalTask newTask) {
        boolean success = taskCache.replace(oldTask.getTaskId(), oldTask, newTask);
        if (!success) {
            // ok, if success was false, that means the task has moved on since we started working on it. That's not good...
            log.error("Completed task '" + oldTask.getTaskId()
                    + "', but it has changed since we started working on it.  \nOur state: '" + oldTask
                    + "'\nCurrent cached state: '" + taskCache.get(oldTask.getTaskId()) + "'");
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Completed task " + oldTask.getTaskId());
            }
        }
    }

    private void markTaskFailed(InternalTask task, Throwable t) {
        InternalTask newTask = new InternalTask(task.getTaskId(), task.getFJTask(), task.getOriginAddress(),
                InternalTask.Status.FAILED, myAddress, t);
        markSuccessOrFailureInCache(task, newTask);
    }

    /**
     * This class watches for changes on the TaskMap and reacts to them. For example, it maintains a
     * queue of available tasks (tasks that might not be taken yet, and are in our partition of the
     * task set).
     */
    @Listener
    public class AvailableTaskListener {

        final AdvancedCache<Object, InternalTask> taskCache;
        final DistributionManager distributionManager;

        public AvailableTaskListener(AdvancedCache<Object, InternalTask> taskCache) {
            this.taskCache = taskCache;
            if (taskCache.getAdvancedCache().getDistributionManager() != null) {
                distributionManager = taskCache.getAdvancedCache().getDistributionManager();
            } else {
                distributionManager = null;
            }
        }

        @CacheEntryCreated
        public void handleNewEntry(CacheEntryCreatedEvent<Object, InternalTask> event) {
            if (!event.isPre()) {
                Object key = event.getKey();
                if (isLocal(key)) {
                    try {
                        availableTasks.put(key);
                    } catch (Throwable t) {
                        log.error("Error while adding key " + key + " to the available tasks queue. This is not terminal, but this task won't be seen as available to execute on this node.", t);
                    }
                }
            }
        }

        @CacheEntryModified
        public void handleUpdatedEntry(CacheEntryModifiedEvent<Object, InternalTask> event) {
            if (!event.isPre()) { // we only care about it after it's done.
                InternalTask task = event.getValue();
                if (task != null && task.isComplete()) {
                    if (isLocal(task.getTaskId())) {
                        // in case we had this in our queue, remove it.
                        availableTasks.remove(task.getTaskId());
                    }
                    // look up our version of this task, if it exists. If it doesn't exist, it wasn't our task.
                    DistributedFJTask ourTask = taskIdToTaskMap.get(task.getTaskId());
                    if (ourTask != null) {
                        // hey, one of my tasks is done!
                        // the event has a task, but it could have a Throwable, too, in case an error was thrown while executing this task.
                        if (event.getValue().getThrowable() != null) {
                            ourTask.completeExceptionally(task.getThrowable());
                        } else {
                            try {
                                ourTask.complete(task.getFJTask().get());
                            } catch (InterruptedException e) {
                                // this shouldn't happen, since it's already complete when we get it.
                                ourTask.completeExceptionally(e);
                            } catch (ExecutionException e) {
                                // if the task threw an exception, we want our copy of the same task to throw that same exception.
                                ourTask.completeExceptionally(e.getCause());
                            }
                        }
                        // clean up: remove this task from our maps.
                        cleanupTaskMap(task.getTaskId());
                    }
                }
            }
        }

        private void cleanupTaskMap(Object taskId) {
            taskIdToTaskMap.remove(taskId);
        }

        @DataRehashed
        public void handleRehash(DataRehashedEvent<Object, InternalTask> event) {
            // TODO: handle rehash. Our "available tasks" queue no longer reflects all the tasks available to us. Needs to be cleared and rebuilt.
        }

        public boolean isLocal(Object key) {
            if (distributionManager != null) {
                return distributionManager.getLocality(key).isLocal();
            } else {
                return taskCache.getAdvancedCache().getDataContainer().containsKey(key);
            }
        }
    }

    private class CompletionQueueWatcher implements Runnable {
        private static final int WARNING_THRESHOLD = 10;
        private static final int WARN_EVERY_MS = 5000; // 5 seconds.

        final BlockingQueue<InternalTask> completedTaskQueue;
        long lastWarning = 0;

        public CompletionQueueWatcher(BlockingQueue<InternalTask> completedTaskQueue) {
            this.completedTaskQueue = completedTaskQueue;
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    if (completedTaskQueue.size() > WARNING_THRESHOLD
                            && (lastWarning + WARN_EVERY_MS) < System.currentTimeMillis()) {
                        log.warn("Warning: our completed task queue is falling behind. It currently has "
                                + completedTaskQueue.size() + " tasks waiting to be ack'd.");
                    }
                    InternalTask task = completedTaskQueue.take();
                    try {
                        task.getFJTask().get();
                        markTaskComplete(task);
                    } catch (Throwable t) {
                        // We don't currently give any exceptions special treatment. We may at one point want to
                        // do something more useful with, say, an InterruptedException or CancellationException...
                        markTaskFailed(task, t);
                    }

                } catch (InterruptedException e) {
                    log.info("Interrupted; shutting down queue watcher.");
                }

            }
        }

    }

    /**
     * This is running on the stealing node's side; it is watching the stolen tasks to see when it
     * completes, and when it does, putting it in the completedTaskQueue so that we'll send the
     * results back.
     *
     * We're counting on this class running in a scheduled thread.
     *
     * This might be more efficiently handled as a callback-based algorithm, rather than tying up a
     * thread to poll the tasks. It would also guarantee immediate response. However, this lets us
     * time out tasks if necessary. Tradeoffs, always tradeoffs...
     */
    private class TaskResultWatcher implements Runnable {
        final ConcurrentMap<InternalTask, WatchingStats> tasks = CollectionFactory.makeConcurrentMap();
        final BlockingQueue<InternalTask> completedTaskQueue;

        public TaskResultWatcher(BlockingQueue<InternalTask> completedTaskQueue) {
            this.completedTaskQueue = completedTaskQueue;
        }

        @Override
        public void run() {
            try {
                // we're using one of the JDK's ConcurrentHashMap implementations, which guarantee consistent iterator access.
                Iterator<Entry<InternalTask, WatchingStats>> iterator = tasks.entrySet().iterator();
                while (iterator.hasNext()) {
                    Entry<InternalTask, WatchingStats> entry = iterator.next();
                    InternalTask task = entry.getKey();
                    if (task.getFJTask().isDone()) {
                        completedTaskQueue.put(task);
                        iterator.remove(); // this is safe on a ConcurrentHashMap or ConcurrentHashMapV8.
                    }
                }
            } catch (Throwable t) {
                if (t instanceof InterruptedException
                        || (t instanceof ExecutionException && ((ExecutionException) t).getCause() instanceof InterruptedException)) {
                    log.trace("Thread interrupted; revisting in the next poll interval (but we're probably being shut down)");
                } else {
                    // don't let the thread die. Log an error and move on with life.
                    log.error("Error within the TaskResultWatcher. Exiting, and revisiting on the next poll interval:", t);
                }
            }
        }

        public void addTask(InternalTask task) {
            tasks.put(task, new WatchingStats());
        }

    }

    private static class WatchingStats {
        // nothing here...perhaps we'll want something?
    }

}
