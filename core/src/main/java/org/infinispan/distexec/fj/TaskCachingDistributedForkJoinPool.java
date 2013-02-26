package org.infinispan.distexec.fj;

import static org.infinispan.util.Util.rewrapAsCacheException;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.UUIDKeyGenerator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.distexec.DistributedTaskBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.concurrent.NotifyingFutureAdaptor;
import org.infinispan.util.concurrent.jdk8backported.ForkJoinTask;

/**
 *
 * cache-based alg:
 *
 * <pre>
 * - create a task cache.
 * - have every other DistPool listening for tasks?
 * 		- and for node add/drop events?
 * - tasks added to the TaskCache have an Owner Address field
 * - when they see a new task in their partition, they send that owner a
 * message: "Hey, pick me!"
 * 		-- possibly with some node info, too. Like load,  memory avail., etc.
 * - owners pick the first (or best, or whatever...pluggable "best" alg) and say, "Ok, here you go."
 * - that transfer happens...how? Through the Transfer? Through an update to the cache itself? (update the task
 * w/ a new Owner? ...or new entry in the list of owners)
 * the owner...sees that? Via a listener? And then starts working it? is that abusing listeners?
 *
 * </pre>
 *
 * @author Matt Hoffman
 *
 */
public class TaskCachingDistributedForkJoinPool extends AbstractDistributedForkJoinPool {

   private static final int KEY_QUEUE_SIZE = 100;
   /**
    * We use extra threads for a number of things; watching tasks for completion, sending responses
    * back asynchronously, ...
    */
   final ExecutorService utilityThreadPool = Executors.newCachedThreadPool();

   /**
    * note that we rely on this executorService's behavior: it will run things with a fixed delay,
    * and if one execution blocks (waiting for available work, for example) it will not concurrently
    * execute any additional executions.
    */
   final ScheduledExecutorService scheduledThreadPool;

   /**
    * We use this to generate keys that are guaranteed to map to our local node. That way, when we
    * have a job submitted that doesn't have a cache key attached, we can still add it to the task
    * cache (so that it is distributed, and can be stolen) but we have the chance of working on it
    * locally. Which we would generally prefer.
    */
   final KeyAffinityService keyAffinityService;

   /**
    * the only things we'll put in this cache are tasks that have been created locally and aren't
    * tied to a cache key -- therefore, they don't have any preference for where they're executed.
    * Because they don't have a preference, we'll execute them locally if we get to them first, but
    * we'll also make them available to be stolen in case someone else wants them.
    */
   Deque<DistributedFJTask<?>> localCacheAgnosticTaskDeque = new ArrayDeque<DistributedFJTask<?>>();
   Lock dequeWriteLock = new ReentrantLock();

   /**
    * we have to keep track of tasks that we've generated locally and then handed off to remote
    * nodes to execute, because we'll have other tasks within the ForkJoinPool that are waiting for
    * those tasks to complete (in an Object.wait() sense); when a remote node completes the task, it
    * won't automatically notify those tasks. We need to do that ourselves.
    */
   final ConcurrentMap<Object, DistributedFJTask<?>> taskIdToTaskMap = ConcurrentMapFactory.makeConcurrentMap();

   /**
    * The cache we're using to target tasks; if our tasks are processing cached data, then this will
    * be the cache that holds the data we're processing. If we're not, then we'll just use the cache
    * for its distribution configuration.
    */
   protected final AdvancedCache cache;

   /**
    * The name of the cache we're using to hold tasks.
    */
   protected String taskCacheNameSuffix = "inf.fj.task-cache";
   public static final String defaultTaskCacheConfigurationName = "task-cache";

   /**
    * The actual cache we're using to hold tasks. This ought to be replicated.
    */
   protected final AdvancedCache<Object, InternalTask> taskCache;

   volatile boolean shutdown = false;

   /**
    * we'll use this as a queue of potentially-available tasks. This is a communication channel
    * between our cache listener that is watching for potentially-executable tasks and our fork-join
    * pool watcher, which is looking to put more work in the pool when it's getting low.
    */
   private final BlockingQueue<Object> availableTasks;

   private long pollLength = 10;
   private TimeUnit pollPeriod = TimeUnit.MILLISECONDS;

   final Address myAddress;

   protected AtomicLong distributableExecutedAsOriginator = new AtomicLong(0);
   protected AtomicLong distributableExecutedNotOriginator = new AtomicLong(0); // this is probably overkill; I don't really care if we miss a count now and then.

   public TaskCachingDistributedForkJoinPool(Cache cache) {
      this(cache, Runtime.getRuntime().availableProcessors());
   }

   public TaskCachingDistributedForkJoinPool(Cache cache, int parallelism) {
      super(parallelism);
      this.cache = cache.getAdvancedCache();
      this.taskCache = createTaskCache(cache, createTaskCacheName(cache.getName(), taskCacheNameSuffix));
      this.keyAffinityService = KeyAffinityServiceFactory.newKeyAffinityService(cache, utilityThreadPool,
            new UUIDKeyGenerator(), KEY_QUEUE_SIZE);
      this.availableTasks = new LinkedBlockingQueue<Object>();

      this.taskCache.addListener(new AvailableTaskListener(taskCache));
      scheduledThreadPool = Executors.newScheduledThreadPool(1);
      this.myAddress = getOurAddress(taskCache);
      try {

         // start listening.
         // Normal Executors have no separate "start" method; they start on
         // construction. So we have to do the same.
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
    * @param cache
    * @param cacheName
    * @param taskCacheConfiguration
    * @return
    */
   protected AdvancedCache<Object, InternalTask> createTaskCache(Cache templateCache, String cacheName) {
      AdvancedCache<Object, InternalTask> taskCache;
      if (templateCache.getCacheManager().cacheExists(cacheName)) {
         log.debug("Cache " + cacheName + " already exists. Using that configuration");
         Cache<Object, InternalTask> c = templateCache.getCacheManager().getCache(cacheName, false);
         assertTaskCacheIsValid(c);
         taskCache = c.getAdvancedCache();
      } else {
         log.debug("Cache " + cacheName + " does not exist yet. Loading...");
         Configuration taskCacheConfiguration = getDefaultCacheConfiguration(templateCache).build();
         EmbeddedCacheManager cm = templateCache.getCacheManager();
         Configuration createdConfig = cm.defineConfiguration(cacheName, taskCacheConfiguration);

         // this is in two steps just to make the compiler happy.
         Cache<Object, InternalTask> basicCache = cm.getCache(cacheName, true);
         log.debug("configuration as we expected? " + basicCache.getCacheConfiguration().equals(createdConfig) + "!");
         taskCache = basicCache.getAdvancedCache();
      }
      return taskCache;
   }

   protected static ConfigurationBuilder getDefaultCacheConfiguration(Cache cache) {
      ConfigurationBuilder cb = new ConfigurationBuilder().read(cache.getCacheManager().getDefaultCacheConfiguration());
      cb.clustering().cacheMode(CacheMode.DIST_SYNC).sync()
         .transaction()
            .syncCommitPhase(true)
            .syncRollbackPhase(true)
            .useSynchronization(true)
;
      Configuration existingCacheConf = cache.getCacheConfiguration();

      // inherit the template cache's hash configuration
      cb.clustering().hash().read(existingCacheConf.clustering().hash());

      // inherit JMX configuration
      cb.jmxStatistics().read(existingCacheConf.jmxStatistics());

      // inherit Classloader
      cb.classLoader(existingCacheConf.classLoader());

      // inherit Sites configuration
      cb.sites().read(existingCacheConf.sites());

      return cb;
   }

   private void assertTaskCacheIsValid(Cache<Object, InternalTask> cache) {
      if (cache.getCacheConfiguration().clustering().cacheMode().isDistributed()
            || cache.getCacheConfiguration().clustering().cacheMode().isReplicated()) {
         return;
      } else {
         throw new IllegalArgumentException("Task cache '" + cache.getName()
               + "' is not distributed or replicated. This doesn't make sense for a task cache");
      }
   }

   public void start() {
      scheduledThreadPool.scheduleAtFixedRate(new DistributedForkJoinQueueWatcher(this), 10, 10, TimeUnit.MILLISECONDS);

      //TODO: start up an executor on the other nodes, too?

   }

   @Override
   public <T> DistributedTaskBuilder<T> createDistributedTaskBuilder(Callable<T> callable) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   protected <T> void addWork(ForkJoinTask<T> forkJoinTask) {
      if (!(forkJoinTask instanceof DistributedFJTask)) {
         throw new IllegalArgumentException("task must be distributable");
      }
      // is this task cache-aware?
      Object taskId;
      if (forkJoinTask instanceof KeyAwareDistributedFJTask<?>) {
         List<Object> keys = ((KeyAwareDistributedFJTask) forkJoinTask).keys();
         // if so, add it to the cache using its desired cache key. If this task conforms to multiple,
         // we'll just pick the first. I'm not sure that a more intelligent algorithm would be worth
         // the cost here.
         taskId = keyAffinityService.getCollocatedKey(keys.get(0));
      } else {
         // if not, generate a key meaning "us" and add it to the cache.
         taskId = keyAffinityService.getKeyForAddress(myAddress);
      }
      InternalTask internalTask = new InternalTask(taskId, (DistributedFJTask) forkJoinTask, myAddress);
      taskCache.put(taskId, internalTask);
      taskIdToTaskMap.put(taskId, internalTask.getFJTask());

   }

   @Override
   protected <T> void notifyListenersOfNewTask(ForkJoinTask<T> task) {
      // TODO Auto-generated method stub

   }

   @Override
   public <T> Future<T> submit(Address target, Callable<T> task) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public <T> Future<T> submit(Address target, DistributedTask<T> task) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public <T, K> Future<T> submit(Callable<T> task, K... input) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public <T, K> Future<T> submit(DistributedTask<T> task, K... input) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public <T> List<Future<T>> submitEverywhere(Callable<T> task) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public <T> List<Future<T>> submitEverywhere(DistributedTask<T> task) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public <T, K> List<Future<T>> submitEverywhere(Callable<T> task, K... input) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public <T, K> List<Future<T>> submitEverywhere(DistributedTask<T> task, K... input) {
      // TODO Auto-generated method stub
      return null;
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
               NotifyingFuture<InternalTask> futureTask = getWork();
               if (futureTask != null) {
                  try {
                     InternalTask task = futureTask.get();
                     if (task != null) {
                        if (trackMetrics) {
                           if (myAddress.equals(task.getOriginAddress())) {
                              distributableExecutedAsOriginator.incrementAndGet();
                           } else {
                              distributableExecutedNotOriginator.incrementAndGet();
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
            }
         } catch (Throwable t) {
            if (t instanceof InterruptedException
                  || (t instanceof ExecutionException && ((ExecutionException) t).getCause() instanceof InterruptedException)) {
               log.trace("Thread interrupted; revisting in the next poll interval (but we're probably being shut down)");
            } else {
               // don't let the thread die. Log an error and move on with life.
               log.error("Error within the ForkJoinQueueWatcher. Exiting, and revisiting on the next poll interval:", t);
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
               while (true) {
                  Object taskId = availableTasks.take(); // blocks until something is available.
                  if (taskId != null) {
                     InternalTask task = taskCache.get(taskId);
                     if (task == null) {
                        log.error("task ID '" + taskId
                              + "' is in our availableTasks queue, but not available in the task map.");
                     } else if (task.getStatus() == InternalTask.Status.READY) {
                        // I'd kind of like to check again whether we still need work. But we can't do it here, or this task will just drop on the floor.
                        //
                        // ok, now we need to claim it for ourselves.
                        // we'll use an atomic replace here, trusting that the STATUS is part of the equals() clause of InternalTask,
                        // and the distributed cache will atomically replace that with a CHECKED_OUT version if no one else has gotten to it.
                        InternalTask newTask = new InternalTask(taskId, task.getFJTask(), task.getOriginAddress(),
                              InternalTask.Status.IN_PROGRESS, myAddress, null);
                        boolean successful = taskCache.replace(taskId, task, newTask);
                        if (successful) {
                           startWatcher(newTask);
                           return task;
                        }
                        // if not successful, then someone else has already checked it out. Oh well. Loop around and try the next one.
                     }
                  }
               }
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

   /**
    * Set up a watcher to watch this task and return the response to the originator when available.
    * TODO: this could be a lot more efficient. It will spawn one thread per task we're working on.
    * Consider adding callbacks to the DistributedFJTask itself, perhaps?
    *
    * @param task
    *           that has been checked out (this method won't check it out, it just spins up a
    *           watcher)
    */
   protected void startWatcher(InternalTask task) {
      utilityThreadPool.execute(new TaskResultWatcher(task));
   }

   /**
    * Our internal wrapper around a ForkJoinTask, tracking its state and anything else we need to
    * know when distributing it.
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

      final Status status;

      final DistributedFJTask task;

      final Address originAddress;
      Address executedBy;

      private Throwable exception;

      public InternalTask(Object taskId, DistributedFJTask forkJoinTask, Address originAddress, Status status,
            Address currentlyWorkingOn, Throwable throwable) {
         this.taskId = taskId;
         this.task = forkJoinTask;
         this.status = status;
         this.originAddress = originAddress;
         this.executedBy = currentlyWorkingOn;
         this.exception = throwable;
      }

      public InternalTask(Object taskId, DistributedFJTask forkJoinTask, Address originAddress) {
         this(taskId, forkJoinTask, originAddress, Status.READY, null, null);
      }

      public DistributedFJTask getFJTask() {
         return task;
      }

      public Status getStatus() {
         return status;
      }

      public boolean isComplete() {
         return status == Status.FAILED || status == Status.COMPLETE;
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
               + (executedBy != null ? "executed by=" + executedBy + ", " : "")
               + (task != null ? "task=" + task : "") + "]";
      }


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
                  //TODO: can't add to the queue yet. The task isn't yet committed. Need to wait.
                  availableTasks.put(key);
               } catch (Throwable t) {
                  //FIXME: handle exception
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

   /**
    * This is running on the stealing node's side; it is watching the stolen task to see when it
    * completes, and when it does, sending the appropriate response back to the server.
    *
    * TODO: this would be more efficiently handled as a callback-based algorithm, rather than tying
    * up a thread to poll the task.
    */
   private class TaskResultWatcher implements Runnable {
      private InternalTask task;

      public TaskResultWatcher(InternalTask task) {
         this.task = task;
      }

      @Override
      public void run() {
         boolean done = false;
         DistributedFJTask fjtask = task.getFJTask();
         while (!done && !Thread.interrupted()) {
            try {
               Object o = fjtask.get(pollLength, pollPeriod);
               if (o != null) {
                  markComplete(task);
                  done = true;
               }
            } catch (ExecutionException t) {
               //TODO: add retry logic here?
               markFailed(task, t);
               done = true;
            } catch (InterruptedException t) {
               // we'll break out of the loop, above.
               markFailed(task, t.getCause());
               done = true;
            } catch (CancellationException e) {
               // propagate that cancellation back to the owner.
               markFailed(task, e);
               done = true;
            } catch (TimeoutException e) {
               // this is expected. If we were doing a message-based algorithm here, we'd ping the
               // owner of the task to let them know we were still working. We're not, though, so
               // we'll let this one go by.
            }
         }
      }

      private void markComplete(InternalTask task) {
         InternalTask newTask = new InternalTask(task.getTaskId(), task.getFJTask(), task.getOriginAddress(),
               InternalTask.Status.COMPLETE,
               myAddress, null);
         markSuccessOrFailureInCache(task, newTask);
      }

      protected void markSuccessOrFailureInCache(InternalTask oldTask, InternalTask newTask) {
         boolean success = taskCache.replace(oldTask.getTaskId(), oldTask, newTask);
         if (!success) {
            // ok, if success was false, that means the task has moved on since we started working on it. That's not good...
            log.error("Completed task '" + oldTask.getTaskId()
                  + "', but it has changed since we started working on it.  \nOur state: '" + oldTask
                  + "'\nCurrent cached state: '" + taskCache.get(oldTask.getTaskId()) + "'");
         }
      }

      private void markFailed(InternalTask task, Throwable t) {
         InternalTask newTask = new InternalTask(task.getTaskId(), task.getFJTask(), task.getOriginAddress(),
               InternalTask.Status.FAILED,
               myAddress, t);
         markSuccessOrFailureInCache(task, newTask);
      }
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
}
