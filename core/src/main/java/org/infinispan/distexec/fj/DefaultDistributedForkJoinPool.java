package org.infinispan.distexec.fj;

import static org.infinispan.util.Util.rewrapAsCacheException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.distexec.DistributedTaskBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.concurrent.jdk8backported.ForkJoinTask;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

/**
 * Adds a distributed processing wrapper around a normal ForkJoinPool.
 *
 * There are a lot of options for a distribution algorithm like this. Here are a
 * few:
 *
 * 1.) pure work-stealing. We could hold a local work queue of "distributable"
 * tasks, as a layer above the normal ForkJoinPool's work queues. Then when
 * we're out of work, we could ask our neighbors (or selected neighbors) if they
 * have work. If they do, we steal the task from them, work on it, and then send
 * the response back. This is simple, and should distribute work well, but
 * provides no data locality; we are likely to work on the tasks we create, no
 * matter what type of data they require. Note that in this scenario, tasks
 * aren't shared unless they're stolen. So if a node goes down, other nodes have
 * no way of recovering the tasks in its work queue. Is this a problem? The
 * tasks in its work queue would be either a.) tasks that it originated, or b.)
 * tasks that it stole from other nodes. So this would work if either a.) nodes
 * tracked which tasks were stolen from them, or b.) they informed the
 * originator of the tasks that the task had been * transferred, or c.) all
 * stolen tasks come with a time limit and a heartbeat. In any of those
 * scenarios, SOMEONE would * know and be able to detect which tasks they would
 * need to retry. However, tasks originating on the dead node would be lost
 * forever; that would have to be ok.
 * <p/>
 * 2.) share-everything We could multicast all tasks going in, so that anyone
 * who needs work can request one. Not sure what advantage we'd get out of that
 * design, but it'd be simple to implement.
 * <p/>
 * 3.) partitioned distribution We could distribute all tasks as a distributed
 * hashmap, using a custom hash. The hash value of any given task could be set
 * by the task, according to the partition strategy of the job in question, or
 * could be randomly generated. Then submitting a job means that it would be
 * executed on the worker (or one of the workers) that 'owned' that partition.
 * More network overhead and latency, since each job would be broadcast around
 * the cluster before being executed. And jobs are more likely to move around,
 * meaning still more overhead. But jobs would be evenly distributed and
 * depending on the advantages of cache locality, it could still be a big win.
 * <p/>
 * 4.) compromise distribution We could allow tasks to declare a partition
 * preference if they have one, and none if they don't. Tasks without a
 * preference would be executed using the work-stealing algorithm, #1, and tasks
 * with a partition preference perhaps use something kind of like #3.
 * <p/>
 *
 * Note that we only recover from failures if a work-stealer crashes, not if a
 * task-originator crashes. And chained work stealers can lead to duplicate
 * tasks. So in the face of node failures, we guarantee "at least once"
 * execution. It would be possible to guarantee "exactly once", but it would
 * entail a lot of extra overhead (or perhaps just a much more clever algorithm
 * that I haven't thought of yet -- consider that a challenge!).
 *
 * An example where things can get messy: Node A originates a task, and breaks
 * it into two chunks. Node B takes one of those chunks, and breaks it further
 * into two chunks. Note C takes one of those chunks and starts working on it,
 * and perhaps breaks it down into more chunks. Then Node B goes down. Node C is
 * still working on chunks, and perhaps breaking it down further and
 * distributing tasks, and so on. But it can't report any results to Node B, so
 * it's doing all of its work for naught. Meanwhile, Node A can't do anything
 * but detect the failure of Node B and restart that whole task.
 *
 * To prevent this, we'd need to detect a node failure and cascade-delete any
 * child task that another node was working on. Perhaps have each node delete
 * any tasks that contain the dead node as a parent? To do that, we could: -
 * broadcast a "node is dead" message (or just trust that each node is
 * subscribed to "node is dead!" messages from the cluster framework) - let each
 * node look in its own task map and see if that node was working on something
 * for it. - for each task that the dead node was working on on its behalf,
 * broadcast a "dead task!" message. - anytime a node receives a "dead task!"
 * message, go through and look in its work queue for any tasks that contain
 * that task as a parent. If they exist, remove or cancel them.
 *
 * Alternatively, we could let each task "hop" only once. I.e. if Node A
 * originates a task, and breaks it into two chunks, then Node B takes one of
 * those chunks and breaks it further into two chunks, Node B has to execute
 * both of those chunks. Node C cannot go and steal one; it has already "hopped"
 * once. That would be very suboptimal for narrowly- branching tasks, but would
 * be simpler to implement I suppose.
 *
 * I believe the Storm project has some interesting algorithms for doing this,
 * since it tracks ancestry of tasks. It uses a clever bloom-filter-like
 * algorithm for checking off when child tasks complete, which may or may not be
 * useful here.
 *
 * Checkpointing? Should be automatic in there somewhere...we have automatic
 * "places" to checkpoint.
 *
 *
 * TODO: create a builder, because the options are getting unwieldy.
 */
public class DefaultDistributedForkJoinPool extends AbstractDistributedForkJoinPool {
	// the JGroups channel
	private final Channel channel;

	// "groupName" in JGroups is kind of like "cluster name". Servers form a
	// cluster if they have the same groupName.
	private final String groupName;

	// a View is a snapshot of what servers are in this cluster at this
	// particular point in time.
	private View view;

	// The class that implements the JGroups listener interfaces. This contains
	// the "receive()" method that gets called when a message comes in.
	WorkReceiver workReceiver = new WorkReceiver();

	// This is a map of tasks that we own, that other tasks are working on
	// (tasks that have been "stolen" by other nodes)
	// it is a map of node address -> set of taskIds
   ConcurrentMap<Address, Set<String>> nodeToTaskMap = ConcurrentMapFactory.makeConcurrentMap();
   ConcurrentMap<String, DistributedFJTask<?>> taskIdToTaskMap = ConcurrentMapFactory.makeConcurrentMap();

	/**
	 * The cache we're using to target tasks; usually, this is the task that holds the data we're processing.
	 */
	protected final AdvancedCache cache;

	/**
	 * The name of the cache we're using to hold tasks.
	 */
	protected String taskCacheName = "task-cache";

	/**
	 * The actual cache we're using to hold tasks. This ought to be replicated.
	 */
	protected final AdvancedCache taskCache;

	/**
	 * If a node asks us for work, and we don't have any to offer, we add them
	 * to this list. Then if we do get work, we will notify at least some of
	 * them that we now have work. How many we notify is controlled by the
	 * "maxListenersToNotifyOnNewTask" field below.
	 */
	private List<Address> newTaskListeners = new CopyOnWriteArrayList<Address>();

	/**
	 * The number of other nodes to notify when we get a new task. We only
	 * notify other nodes that have explicitly asked us for work when we didn't
	 * have any available. This is to prevent a <a
	 * href="http://en.wikipedia.org/wiki/Thundering_herd_problem">thundering
	 * herd problem</a> when we do get work.
	 */
	private int maxListenersToNotifyOnNewTask = 5;
	private long waitTimeBetweenStealRequests = 100;// in milliseconds.
	private long pollLength = 10;
	private TimeUnit pollPeriod = TimeUnit.MILLISECONDS;

	private static final String DEFAULT_GROUP_NAME = "dist-work-group";
	private static final String DEFAULT_CONFIGURATION_FILE = "tcp-nio.xml";

	/**
	 * We use extra threads for a number of things; watching tasks for
	 * completion, sending responses back asynchronously, ...
	 */
	ExecutorService utilityThreadPool = Executors.newCachedThreadPool();
	ScheduledExecutorService scheduledThreadPool;

	/**
	 * We try to rotate who we steal from, to keep things distributed.
	 */
	Address lastStealRequestTarget = null;

	/*
	 * metrics we'll be keeping track of, if trackMetrics is true
	 */
	// I'm not sure if there's value in both a meter and an unweighted
	// histogram. The meter tracks a counter and a weighted histogram of rate of
	// arrival.
	// The histogram tracks queue size over time, unweighted.
	// I'm keeping both for the moment; hopefully after we've used them for a
	// while we'll know if the histogram adds value. Note that the metrics are
	// held by name, so all instances of this class on
	// the same JVM will share the same metric.
	// If we really wanted to track per-instance metrics, we'd need an
	// "instance number" in the metric name or something
	// like that.

	// static final Histogram distributableJobsHistogram =
	// Metrics.defaultRegistry().newHistogram(DefaultDistributedForkJoinPool.class,
	// "distributable-jobs-hist", true);
	// static final Meter stolenJobsMeter =
	// Metrics.defaultRegistry().newMeter(DefaultDistributedForkJoinPool.class,
	// "jobs-stolen-meter", "jobs", TimeUnit.SECONDS);
	// static final Meter jobsStolenFromMeMeter = Metrics.defaultRegistry().newMeter(DefaultDistributedForkJoinPool.class, "jobs-stolen-from-me-meter", "jobs", TimeUnit.SECONDS);

	AtomicLong jobsStolenFromMeMeter = new AtomicLong(0);

	/** TODO: this constructor doesn't make sense anymore */
	@Deprecated
	public DefaultDistributedForkJoinPool() {
		this(new DefaultCacheManager(true).getCache(), DEFAULT_GROUP_NAME);
	}

	public DefaultDistributedForkJoinPool(Cache cache, String groupName) {
		this(cache, DEFAULT_CONFIGURATION_FILE, groupName);
	}

	public DefaultDistributedForkJoinPool(Cache cache, String configurationFileName, String groupName) {
		this(cache, configurationFileName, groupName, Runtime.getRuntime().availableProcessors());
	}



	public DefaultDistributedForkJoinPool(Cache cache, String configurationFileName, String groupName, int parallelism) {
		super(parallelism);
		this.cache = cache.getAdvancedCache();
		this.taskCache = getCache(cache, taskCacheName);

		this.groupName = groupName;
		try {
			channel = new JChannel(configurationFileName);
			channel.setReceiver(workReceiver);

			// start listening.
			// Normal Executors have no separate "start" method; they start on
			// construction. So we have to do the same.
			start();
		} catch (Exception e) {
			// TODO: Do something?
			log.error("Error creating channel using configuration file " + configurationFileName + ": ", e);
			throw rewrapAsCacheException(e);
		}
	}

	protected AdvancedCache getCache(Cache cache, String cacheName) {
		AdvancedCache taskCache;
		if (cache.getCacheManager().cacheExists(cacheName)) {
			Cache c = cache.getCacheManager().getCache(cacheName, false);
			assertTaskCacheIsValid(c);
			taskCache = c.getAdvancedCache();
		} else {
			ConfigurationBuilder cb = new ConfigurationBuilder().read(cache.getCacheManager().getDefaultCacheConfiguration());
			cb.clustering().cacheMode(CacheMode.DIST_ASYNC);
			org.infinispan.configuration.cache.Configuration c = cb.build();
			cache.getCacheManager().defineConfiguration(taskCacheName,  c);
			taskCache = cache.getCacheManager().getCache(taskCacheName, true).getAdvancedCache();
		}
		return taskCache;
	}

	private void assertTaskCacheIsValid(Cache cache) {
		if (cache.getCacheConfiguration().clustering().cacheMode().isDistributed() || cache.getCacheConfiguration().clustering().cacheMode().isReplicated()) {
			return;
		} else {
			throw new IllegalArgumentException("Task cache '"+ cache.getName()+"' is not distributed or replicated. This doesn't make sense for a task cache");
		}
	}

	public void start() {
		try {
			// connect to our JGroups cluster
			channel.connect(groupName);
		} catch (Exception e) {
			log.error("Error connecting to group " + groupName
					+ ". This means that work will not be distributed. Error was: ", e);
		}

		scheduledThreadPool = Executors.newScheduledThreadPool(1);
		scheduledThreadPool.scheduleAtFixedRate(new DistributedForkJoinQueueWatcher(this), 10, 10,
				TimeUnit.MILLISECONDS);
	}

	/**
	 * Returns true if this WorkManager is connected to the cluster (even if
	 * it's just a cluster of one) and able to receive work.
	 *
	 * @return
	 */
	public boolean isRunning() {
		return channel.isConnected();
	}

	@Override
	public void shutdown() {
		stop();
		super.shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		stop();
		return super.shutdownNow();
	}

	/**
	 * Disconnect from the cluster. Once "stop" has been called, this server can
	 * no longer receive work.
	 */
	protected void stop() {
		try {
			channel.disconnect();
		} catch (Throwable t) {
			log.warn("error while disconnecting from channel; will attempt to continue: ", t);
		}
		try {
			channel.close();
		} catch (Throwable t) {
			log.warn("error while closing channel; will attempt to continue: ", t);
		}
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
	}

	/**
	 * A "view" is JGroups's term for a snapshot of which nodes are connected to
	 * this cluster. This method is called when our cluster membership listener
	 * has detected a change in the cluster. If we need to do something in
	 * particular when cluster membership changes, this is a good place to put
	 * it.
	 *
	 * @param view
	 */
	protected void setView(View view) {
		if (log.isDebugEnabled()) {
			log.debug("Cluster membership changed. \nPreviously: " + this.view + ". \nNow:        " + view);
		}
		List<Address> leftMembers = Util.leftMembers(this.view, view);
		handleLeftMembers(leftMembers);
		this.view = view;
	}

	/**
	 * When a node leaves the cluster, we need to look to see if they were
	 * working on any tasks for us. If they were, we'll put them back in our
	 * distributed queue, so they'll get worked on.
	 * <p/>
	 * Note that is essentially assuming that a task can be retried. We can't
	 * guarantee that this task hasn't been executed already on the other node,
	 * and it failed just before sending us the response. Or it's still alive,
	 * but a network problem has prevented it from responding to heartbeats. We
	 * are erring on the side executing the task *at least once*, and leaving it
	 * to the programmer to make tasks idempotent.
	 *
	 * @param leftMembers
	 */
	private void handleLeftMembers(List<Address> leftMembers) {
		if (leftMembers != null) {
			for (Address member : leftMembers) {
				retryTasksForMember(member);
			}
		}
	}

	private void retryTasksForMember(Address member) {
		Set<String> taskIds = nodeToTaskMap.get(member);
		if (taskIds != null) {
			synchronized (taskIds) {
				if (!taskIds.isEmpty()) {
					log.debug("putting " + taskIds.size() + " tasks back in the queue that member " + member
							+ " had been working on");
					for (String taskId : taskIds) {
						DistributedFJTask<?> task = getStolenTaskById(taskId, member);
						if (task != null) {
							// we know it's distributable, we can bypass that
							// check and call submitDistributable directly.
							submitDistributable(task);
							// don't need to hold it in the map anymore.
							taskIdToTaskMap.remove(taskId);
						}
					}
				}
			}
		}
		nodeToTaskMap.remove(member);
	}

	private DistributedFJTask<?> getStolenTaskById(String taskId, Address them) {
		DistributedFJTask<?> ourTask = taskIdToTaskMap.get(taskId);
		if (ourTask == null) {
			log.error("Server "
					+ them
					+ " is telling us they've completed task "
					+ taskId
					+ ", but I can't find a reference to that task. If we restarted recently, perhaps this task was in progress before we restarted?");
		}
		return ourTask;
	}

	protected View getView() {
		return view;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder("work manager: ");
		sb.append(this.channel.getAddress()).append("/").append(this.groupName);
		sb.append("current cluster members: ").append(view.toString());
		// total executed tasks? queued tasks? other statistics?
		return sb.toString();
	}

	@Override
	protected <T> void notifyListenersOfNewTask(ForkJoinTask<T> task) {
		byte[] msg = createNewTaskAvailableMessage(task);
		if (msg != null) {
			int notified = 0;
			if (newTaskListeners != null && newTaskListeners.isEmpty()) {
				Iterator<Address> listenerIterator = newTaskListeners.iterator();
				List<Address> toRemove = new ArrayList<Address>(maxListenersToNotifyOnNewTask);
				while (listenerIterator.hasNext() && notified < maxListenersToNotifyOnNewTask) {
					Address node = listenerIterator.next();
					try {
						channel.send(node, msg);
						toRemove.add(node);
					} catch (Exception e) {
						log.warn(
								"Error notifying node "
										+ node
										+ " of new work. This is non-terminal: we will continue attempting to notify other nodes. The error was: ",
								e);
						toRemove.add(node);
					}
				}
				// they've been notified; no need to notify them again.
				newTaskListeners.removeAll(toRemove);
			}
		}
	}

	private <T> byte[] createNewTaskAvailableMessage(ForkJoinTask<T> task) {
		NewTaskAvailable msg = new NewTaskAvailable();
		try {
			return Util.objectToByteBuffer(msg);
		} catch (Exception e) {
			log.warn(
					"Failed to serialize NewTaskAvailable message. This is non-terminal, but workers won't be notified that we have work available. Error was: : ",
					e);
			return null;
		}
	}

	/*
	 * public DistributedFJTask getWork(NodeId nodeId) {
	 *
	 * }
	 */

	// methods relating to distribution:
	protected void sendStealRequest() {
		// Q: This is called from a separate thread. Is that bad?
		// A: no, JChannel.send() is thread-safe. See
		// http://sourceforge.net/p/javagroups/discussion/130427/thread/fdc5a8ea

		// figure out who we should try to steal from.
		List<Address> potentialTargets = getNextTarget(lastStealRequestTarget);
		// we only want to send one message, but in case we have trouble
		// reaching one, we'll move on to the next.
		for (Address potentialTarget : potentialTargets) {
			if (log.isTraceEnabled()) {
				log.trace(channel.getAddress() + " sending a StealWorkRequest to " + potentialTarget);
			}
			StealWorkRequest request = new StealWorkRequest(channel.getAddress());
			// create a Steal Work request
			// send it off
			try {
				channel.send(potentialTarget, request);
				break; // we only want to send one message. This one was
						// successful; we can stop.
			} catch (Exception e) {
				log.warn("Error sending message to " + potentialTarget + "; moving on to next target. Problem was: "
						+ e.getMessage());
			}
			// well, that's kind of annoying. It's not synchronous, of course.
			// So...we wait? To see if we get a task back?
		}
		// and we're done.
	}

	/**
	 * when we change from using a deque to something more interesting, this
	 * will have to change. It's abstracted away here into a separate method in
	 * anticipation of that.
	 *
	 * @param src
	 */
	private DistributedFJTask<?> getWorkForNode(Address src) {
		if (!hasQueuedSubmissions()) {
			// I am short on work myself. Don't send anything.
			return null;
		} else {
			dequeWriteLock.lock();
			try {
				return deque.pollLast();
			} finally {
				dequeWriteLock.unlock();
			}
		}
	}

	private FJMessage extractMessage(Message message) {
		byte[] bytes = message.getBuffer();
		Object o = null;
		try {
			o = Util.objectFromByteBuffer(bytes);
		} catch (Exception e) {
			log.warn("Could not extract message from " + message + ". Error was: ", e);
		}
		return (FJMessage) o;
	}

	private List<Address> getNextTarget(Address lastStealRequestTarget) {
		// view can be null if JGroups hasn't connected
		if (view != null) {
			List<Address> members = view.getMembers();
			Address target = lastStealRequestTarget;
			if (target == null || !members.contains(target)) {
				// if lastStealRequestTarget isn't valid, use our own address.
				// As good a default as any.
				target = channel.getAddress();
			}
			// make sure target is in that list.
			// create a new list of everyone after us + everyone before us. This
			// gives us a consistent order of people to steal from.
			return reorderListFromEntry(members, target);
		} else {
			return Collections.emptyList();
		}
	}

	/**
	 * This is split out for ease of testing.
	 *
	 * @param members
	 * @param me
	 * @param <T>
	 * @return
	 */
	protected <T> List<T> reorderListFromEntry(List<T> members, T me) {
		int index = members.indexOf(me);
		if (index != -1) { // this would make no sense
			List<T> targets = new ArrayList<T>(members.size() - 1);
			targets.addAll(members.subList(index + 1, members.size()));
			targets.addAll(members.subList(0, index));
			return targets;
		} else {
			log.warn("Error: couldn't find ourself in the list of cluster members. This should not happen. Attempting to proceed.");
			return members;
		}
	}

	/**
	 * This is the thread that watches the inner ForkJoinPool, tries to detect
	 * when it's empty (or almost empty), and pulls a task from our pool of
	 * distributable tasks to give it.
	 */
	protected class DistributedForkJoinQueueWatcher implements Runnable {
		final DefaultDistributedForkJoinPool forkJoinPool;

		long lastStealRequest = 0;

		public DistributedForkJoinQueueWatcher(DefaultDistributedForkJoinPool forkJoinPool) {
			this.forkJoinPool = forkJoinPool;
		}

		@Override
		public void run() {
			try {
				// - queue watcher thread will watch the wrapped FJ submission
				// queue and see if it's empty. If it's empty, and the queue
				// isn't,
				// we'll pop the first task off the queue (FIFO) and put it the
				// fJ queue.
				// we'll then wait slightly longer, on the assumption that a
				// DistributedFJTask is fairly large (large enough that it *may*
				// be able to saturate our worker threads),
				// but it may take us a short while to get it broken down into
				// candidate pieces. So we try to give it a minute.
				// - we also keep track of (?) how many of those
				// DistributedFJTasks we're working at one time.
				if (poolNeedsWork()) {
					// coming up next: replace the block below with this,
					// effectively moving the deque-specific logic into a
					// "getWork" method.
					// need to figure out how to handle the work stealing bit,
					// though.
					// ListenableFuture<ForkJoinTask<?>> task = getWork();
					// task.addListener( if not null, submit directly );

					if (!forkJoinPool.deque.isEmpty()) {
						// lock the deque, since we're going to be writing to
						// it.
						forkJoinPool.dequeWriteLock.lock();
						try {
							// if there's still a task available
							ForkJoinTask<?> task = forkJoinPool.deque.pollFirst();
							if (task != null) {
								// submit it to the ForkJoinPool
								forkJoinPool.submitDirectly(task);
							}
						} finally {
							forkJoinPool.dequeWriteLock.unlock();
						}
					} else if (lastStealRequest + waitTimeBetweenStealRequests <= System.currentTimeMillis()) {
						// The fj pool is empty and the distributable workqueue
						// is empty. We're bored. Idle hands have sticky
						// fingers.
						// Is there someone else out there that has some work we
						// can take?
						sendStealRequest();
						// it's a little tough to continue the metaphor of
						// "stealing" when you're sending out a request.
						// "Good sir, do you have anything I can steal?"
						// "Why, yes indeed. Here you go!"
						// "Ah, jolly good. Thanks!"
						lastStealRequest = System.currentTimeMillis();
					}
				}
				// while we're here, let's update our histogram
				if (trackMetrics) {
					// note that size is a very cheap operation with an
					// ArrayDeque. If the deque implementation changes,
					// this may need to be revisited.
					// distributableJobsHistogram.update(deque.size());
				}
			} catch (Throwable t) {
				// don't let the thread die. Log an error and move on with life.
				log.error("Error within the ForkJoinQueueWatcher. Exiting, and revisiting on the next poll interval:",
						t);
			}
		}

	}

	// Note that this deque, and the lock protecting it, are hopefully entirely
	// temporary.
	// It's fast and simple for prototype purposes, but it's also grossly
	// inefficient in a whole host of ways. It needs
	// to go away, soon. Consider that a challenge to you, oh reader: revise
	// this into something better!
	// Some ideas:
	// There's absolutely nothing that says this class has to hold onto tasks in
	// a Deque-like structure at all, and
	// hand out work to potential work thieves in LIFO order.
	// We could just as well get some kind of consistent hash of the task
	// (perhaps a method in DistributedFJTask?)
	// and hold onto tasks in a LinkedHashMap of tasks by partition. Then
	// callers are matched up to a hash bucket
	// in some deterministic way (like Dynamo's/Riak's consistent hash
	// algorithm), so always get the same hash buckets
	// if they're present.
	// If they're not present, perhaps they can get anything. Perhaps the task
	// itself can say how costly it might be
	// to be executed by a non-preferred node. That way, if a task reads in a
	// lot of data that is likely to be in cache,
	// it can express that it's worth waiting a bit and handing it off to a node
	// that might have it in cache (the
	// "preferred" node) rather than executing it on the first available node.
	// In addition, perhaps when we hand tasks to our own workers, we also pull
	// from our own preferred partitions
	// first if available.

	Deque<DistributedFJTask<?>> deque = new ArrayDeque<DistributedFJTask<?>>();

	Lock dequeWriteLock = new ReentrantLock();

	@Override
	protected <T> void addWork(ForkJoinTask<T> task) {
		dequeWriteLock.lock();
		try {
			deque.push((DistributedFJTask<?>) task);
		} finally {
			dequeWriteLock.unlock();
		}

	}

	protected NotifyingFuture<ForkJoinTask<?>> getWork() {
		// TODO: implement me. This should be an analog of addWork, and used
		// instead of any direct call to deque.poll or .get or whatever.
		return null;
	}

	protected void cleanupTaskMaps(String taskId, Address node) {
		// clean up our maps
		Set<String> taskIds = nodeToTaskMap.get(node);
		if (taskIds != null) {
			synchronized (taskIds) {
				taskIds.remove(taskId);
			}
		}
		taskIdToTaskMap.remove(taskId);
	}

	private class WorkReceiver extends ReceiverAdapter {

		@Override
		public void viewAccepted(View view) {
			setView(view);
		}

		@Override
		public void receive(Message message) {
			FJMessage msg = extractMessage(message);

			// if this is a "give me work!" message,
			if (msg instanceof StealWorkRequest) {
				handleStealWorkRequest((StealWorkRequest) msg, message);
			} else if (msg instanceof WorkResult) {
				// if this is a "here's a response" message,
				handleWorkResult((WorkResult) msg, message);
			} else if (msg instanceof StealWorkResponse) {
				// if this is a "here's a task" message,
				handleStealWorkResponse((StealWorkResponse) msg, message);
			} else if (msg instanceof StillWorking) {
				// is this necessary? We know when the server dies.
				handleStillWorking((StillWorking) msg, message);
			}

		}

		private void handleStealWorkRequest(StealWorkRequest msg, Message message) {
			// ok, they want work.
			// Moochers.

			// do we have work?
			// if so. pull a task off the back of the deque.
			boolean notified = false;
			DistributedFJTask<?> task = null;
			try {
				task = getWorkForNode(message.getSrc());

				// task == null means "no, we don't."
				if (task != null) {
					if (log.isTraceEnabled()) {
						log.trace("Server " + message.getSrc() + " is looking for work; giving him " + task);
					}

					// hold onto our version of this task in our local map
					taskIdToTaskMap.put(task.getId(), task);

					// note that this node has taken this task
					Set<String> taskIds = getOrCreateSet(nodeToTaskMap, message.getSrc());
					synchronized (taskIds) {
						taskIds.add(task.getId());
					}
					if (trackMetrics) {
						 jobsStolenFromMeMeter.incrementAndGet();
					}
					// According to JGroups docs, it's a huge taboo to send a
					// message back in the same thread that is doing the
					// receiving.
					// So we'll spawn a thread to send this one.
					utilityThreadPool.submit(new StealWorkResponseMessageSender(channel, message.getSrc(), task));
				} else {
					log.trace("Server " + message.getSrc() + " is looking for work, but I have none to give him");
				}
			} catch (Throwable t) {
				// I don't see any code there that could throw an exception, but
				// just in case... if we did throw an exception, I don't want to
				// lose the task.
				if (task != null && !notified) {
					log.error("Error trying to handle a StealWorkRequest from " + message.getSrc()
							+ "; putting that task back on the queue. Error was: ", t);
					submitDistributable(task);
				}
			}

		}

		private void handleStillWorking(StillWorking msg, Message message) {
			// if we didn't have a heartbeat mechanism, we would make the
			// servers that are working on tasks for us send back a
			// "I'm still working on it!" response every hundred milliseconds or
			// so, so we knew if it died.
			// However, with JGroups we have a heartbeat mechanism to take care
			// of that, so this isn't necessary right now.
		}

		private void handleStealWorkResponse(StealWorkResponse msg, Message message) {
			DistributedFJTask<?> task = msg.getTask();
			// register that task with a watcher. That's going to detect when
			// this task completes and send the completed
			// task back to the server we stole it from.
			// yes, this could lead to a lot of threads. I'm open to better
			// alternatives.
			if (task != null) {
				if (trackMetrics) {
					// stolenJobsMeter.mark();
				}
				// the queue was empty a second ago, but we aren't submitting
				// directly. Perhaps we should,
				// but since this is asynchronous, we have to admit the
				// possibility that we could have sent out more than
				// one steal request, and received more than one task.
				// So we'll go ahead and queue them up properly.
				submitDistributable(task);
				// now, let's set up a watcher to watch this task and return the
				// response to the originator when available.
				TaskResultWatcher watcher = new TaskResultWatcher(task, message.getSrc());
				utilityThreadPool.execute(watcher);
			}
		}

		@SuppressWarnings({ "ThrowableResultOfMethodCallIgnored", "unchecked", "rawtypes" })
		private void handleWorkResult(WorkResult msg, Message message) {
			if (log.isTraceEnabled()) {
				log.trace("Got a response for task " + msg.getTask().toString() + " back from server "
						+ message.getSrc().toString());
			}
			// look up our local version of this task and set the response.
			DistributedFJTask<? extends Serializable> theirTask = msg.getTask();
			DistributedFJTask ourTask = getStolenTaskById(theirTask.getId(), message.getSrc());
			if (ourTask == null) {
				log.error("Cannot process result of task " + theirTask + ": task not found!");
				return;
			}
			// clean up: remove this task from our maps.
			cleanupTaskMaps(theirTask.getId(), message.getSrc());
			// msg has a task, but it could have a throwable, too, in case an
			// error was thrown while executing this task.
			if (msg.getThrowable() != null) {
				ourTask.completeExceptionally(msg.getThrowable());
			} else {
				try {
					ourTask.complete(theirTask.get());
				} catch (InterruptedException e) {
					// this shouldn't happen, since it's already complete when
					// we get it.
					ourTask.completeExceptionally(e);
				} catch (ExecutionException e) {
					// if the task threw an exception, we want our copy of the
					// same task to throw that same exception.
					ourTask.completeExceptionally(e.getCause());
				}
			}
		}

		/**
		 * This is atomic; we won't overwrite a set that already exists. At
		 * least, that's the hope.
		 *
		 * @param nodeToTaskMap
		 * @param src
		 * @return
		 */
		private Set<String> getOrCreateSet(ConcurrentMap<Address, Set<String>> nodeToTaskMap, Address src) {
			if (!nodeToTaskMap.containsKey(src)) {
				nodeToTaskMap.putIfAbsent(src, new HashSet<String>());
			}
			return nodeToTaskMap.get(src);
		}

	}

	/**
	 * marker superclass.
	 */
	public static class FJMessage {

	}

	protected static class StealWorkRequest extends FJMessage implements Streamable {
		Address originator;

		// no-arg constructor for serialization purposes
		public StealWorkRequest() {
		}

		public StealWorkRequest(Address address) {
			this.originator = address;
		}

		public Address getOriginator() {
			return originator;
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append("StealWorkRequest");
			sb.append("{originator=").append(originator);
			sb.append('}');
			return sb.toString();
		}

		@Override
		public void writeTo(DataOutput out) throws Exception {
			Util.writeAddress(originator, out);
		}

		@Override
		public void readFrom(DataInput in) throws Exception {
			originator = Util.readAddress(in);
		}
	}

	protected static class WorkResult extends FJMessage implements Streamable {
		private DistributedFJTask task;
		private Throwable throwable;

		public WorkResult(DistributedFJTask task) {
			this.task = task;
		}

		public WorkResult(DistributedFJTask task, Throwable t) {
			this.task = task;
			this.throwable = t;
		}

		// no-arg constructor for serialization purposes.
		public WorkResult() {
		}

		public DistributedFJTask getTask() {
			return task;
		}

		public Throwable getThrowable() {
			return throwable;
		}

		@Override
		public void writeTo(DataOutput out) throws Exception {
			Util.writeObject(task, out);
			Util.writeObject(throwable, out);
		}

		@Override
		public void readFrom(DataInput in) throws Exception {
			task = (DistributedFJTask) Util.readObject(in);
			throwable = (Throwable) Util.readObject(in);
		}
	}

	protected static class StealWorkResponse extends FJMessage implements Streamable {
		DistributedFJTask<?> task;

		// no-arg constructor for serialization purposes.
		public StealWorkResponse() {
		}

		public StealWorkResponse(DistributedFJTask<?> task) {
			this.task = task;
		}

		public DistributedFJTask<?> getTask() {
			return task;
		}

		@Override
		public void writeTo(DataOutput out) throws Exception {
			Util.writeObject(task, out);
		}

		@Override
		public void readFrom(DataInput in) throws Exception {
			task = (DistributedFJTask) Util.readObject(in);
		}
	}

	/**
	 * TODO: not sure this is necessarily, since we're doing heartbeats.
	 */
	protected static class StillWorking extends FJMessage implements Streamable {
		private String taskId;

		public StillWorking() {
		}

		public StillWorking(DistributedFJTask task) {
			this.taskId = task.getId();
		}

		@Override
		public void writeTo(DataOutput out) throws Exception {
			Util.writeString(taskId, out);
		}

		@Override
		public void readFrom(DataInput in) throws Exception {
			taskId = Util.readString(in);
		}
	}

	/**
	 * We send this to nodes that have recently asked us for work when we didn't
	 * have any. It just lets them know that we now have work available.
	 */
	protected static class NewTaskAvailable extends FJMessage implements Streamable {
		private String taskId;

		public NewTaskAvailable() {
		}

		public NewTaskAvailable(DistributedFJTask task) {
			this.taskId = task.getId();
		}

		@Override
		public void writeTo(DataOutput out) throws Exception {
			Util.writeString(taskId, out);
		}

		@Override
		public void readFrom(DataInput in) throws Exception {
			taskId = Util.readString(in);
		}
	}

	/**
	 * This is running on the stealing node's side; it is watching the stolen
	 * task to see when it completes, and when it does, sending the appropriate
	 * response back to the server.
	 */
	private class TaskResultWatcher implements Runnable {
		private Address address;
		private DistributedFJTask task;

		public TaskResultWatcher(DistributedFJTask task, Address src) {
			this.task = task;
			this.address = src;
		}

		// no-arg constructor for serialization purposes.
		public TaskResultWatcher() {
		}

		@Override
		public void run() {
			boolean done = false;
			while (!done && !Thread.interrupted()) {
				try {
					Object o = task.get(pollLength, pollPeriod);
					if (o != null) {
						send(address, new WorkResult(task));
						done = true;
					}
				} catch (ExecutionException t) {
					send(address, new WorkResult(task, t.getCause()));
					done = true;
				} catch (InterruptedException t) {
					// we'll break out of the loop, above.
					send(address, new WorkResult(task, t.getCause()));
					done = true;
				} catch (CancellationException e) {
					// propagate that cancellation back to the owner.
					send(address, new WorkResult(task, e));
					done = true;
				} catch (TimeoutException e) {
					// this is expected. We want to send a ping to the owner,
					// letting them know we're still working,
					// then go back into the loop
					send(address, new StillWorking(task));
				}
			}
		}

		private void send(Address address, FJMessage workResult) {
			try {
				channel.send(address, workResult);
			} catch (Exception e) {
				log.error("Error sending result of task " + task + " to " + address + ". This isn't good.");
				// TODO: retry, with backoff? Do something else? Broadcast
				// something?
			}
		}
	}

	private class StealWorkResponseMessageSender implements Runnable {
		private Channel channel;
		private Address sendTo;
		private DistributedFJTask<?> task;

		public StealWorkResponseMessageSender(Channel channel, Address sendTo, DistributedFJTask<?> task) {
			this.channel = channel;
			this.sendTo = sendTo;
			this.task = task;
		}

		@Override
		public void run() {
			// ok. hand it off, then.
			StealWorkResponse resp = new StealWorkResponse(task);

			// now send the task to the node
			try {
				channel.send(sendTo, resp);
			} catch (Throwable e) {
				log.error("Error sending StealWorkResponse to " + sendTo
						+ "; putting that task back on the queue. Error was: ", e);
				submitDistributable(task);
				cleanupTaskMaps(task.getId(), sendTo);
			}
		}
	}

	@Override
	public <T> Future<T> submit(org.infinispan.remoting.transport.Address target, Callable<T> task) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Future<T> submit(org.infinispan.remoting.transport.Address target, DistributedTask<T> task) {
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

	@Override
	public <T> DistributedTaskBuilder<T> createDistributedTaskBuilder(Callable<T> callable) {
		// TODO Auto-generated method stub
		return null;
	}
}
