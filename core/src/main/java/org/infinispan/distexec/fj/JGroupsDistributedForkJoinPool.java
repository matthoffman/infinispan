package org.infinispan.distexec.fj;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.concurrent.FutureListener;
import org.infinispan.util.concurrent.jdk8backported.ForkJoinTask;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static org.infinispan.util.Util.rewrapAsCacheException;

/**
 * This is heavily inspired by Bela Ban's paper on Task Distribution in JGroups (http://www.jgroups.org/taskdistribution.html)
 * and the discussion on the infinispan-dev list here:
 * http://www.mail-archive.com/infinispan-dev@lists.jboss.org/msg05135.html
 *
 * Concretely, it borrows the idea of hashing the task and targeting it to a node based on that hash. The task distribution
 * paper uses a random number which is then mapped to a single node using the consistent view of members provided by
 * JGroups. In the interests of scalability, and since we have additional facilities provided by Infinispan, we'll use
 * something a little more complicated.
 *
 * The overall design is this:
 *
 * The ForkJoinPool is passed a cache instance on construction. We'll use that cache for two things: first, if tasks
 * want to operate on data that is within that cache, we will attempt to target the task to the cache node that contains
 * the cached data.  Second, we will use the associated consistent hashing and distribution configuration of that cache
 * to distribute tasks. More details on that later. This allows callers to configure exactly how we distribute tasks,
 * how many nodes in the cluster can fail, whether we are topology-aware, and so on, by using the standard cache
 * configuration mechanisms.
 *
 * We have a cluster of nodes, and each node can submit tasks to be executed by the cluster.
 * When submitting a task, we first check to see if the task has associated cache keys -- meaning, does this task expect
 * to operate on data that is within the cache. If so, we will generate a task ID that maps to the same partition as that
 * cache key. If it is not, we will generate a random taskId.
 *  -- TODO: make this behavior pluggable and rewrite this doc
 *
 * Given the task ID, we will use the consistent hashing function associated with the this consistent hash, we will then
 * find the owners of that hash (using the distribution configuration of the cache provided during construction) and
 * send the task to those owners (an EXECUTE message). The EXECUTE message includes the task to be executed and the list
 * of recipients.
 *
 * The recipients of an EXECUTE message add the message in their in-memory map of tasks in progress, along with the
 * submitter's address.
 *
 * Each of the nodes receiving the EXECUTE message checks to see if they are at the top of the list of recipients of
 * the message. If so, they execute the task and return the result to the submitter (a RESULT message).
 *
 * When the submitter receives the response, it broadcasts a REMOVE message to each of the original recipients (not the
 * current owners based on the consistent hash, because that could have changed). Those nodes then remove the message
 * from their map of tasks in progress.
 *
 * If a node X crashes (or leaves gracefully), every node looks to see whether it has any tasks owned by X in their map
 * of active tasks. If they exist, the node then looks to see if they are next on the list of recipients that was sent
 * with that message. If so, they become the new executor of that task.
 *
 * Why do we do it this way, instead of letting each node use their consistent hash to see if they are the current owner
 * of the partition?
 * Mainly because we cannot (I think?) guarantee that the list of owners hasn't changed since the message was sent. For
 * example:
 * What if the submitter hashes the task and sends it to (A,B,C), where A is the primary.  But D joins the cluster before
 * A gets the message, and D now ought to be primary. A, B, and C each receive the message, check and see that they are
 * not the owners, and just save the message away as "being executed by D".  However, D didn't even receive the message
 * -- they hadn't yet joined when the message was sent. We could work around that with vector clocks on messages and
 * node membership events to establish "happens-before", but just sending the list of who should execute seems simpler.
 *
 * All things being equal, I'd like to avoid sending the list of participants with the message, to save that few bytes.
 * How many?  Not many. But still...
 *
 * We could avoid this by always sending the message to *all* servers, and consistent hashing on receipt. But in that
 * case, when D joins the cluster before A gets the message, if D does not also get the message (because it had not yet
 * joined when the message was sent), then we still have problems.
 *
 * In the future, we could send two different messages:
 * EXECUTE_MASTER
 * EXECUTE_BACKUP
 * where EXECUTE_BACKUP has the list of recipients in it, but EXECUTE_MASTER doesn't. Save a few bytes for that one
 * message. But that additional complexity is not (yet) worth it for that few bytes for a single recipient.
 *
 * I'm still not a huge fan of push-based distribution; pull-based handles servers of different specs better. Not sure
 * if we can handle that at all.
 *
 * Could add a random assignment element with akka-like load reporting...
 *
 *
 *
 *
 *
 * If a task is created and it is going to process data that is in the cache, then we'll use the same consistent hash
 * function that the cache is using. That way, the task should be executed on the same node that contains the cached
 * data. If a cache is distributed in such a way that multiple nodes contain the same data, we'll attempt to target the
 * one with the lowest load.
 *
 * If a task is created that isn't going to process data that is in the cache, things get more interesting. We use a
 * pluggable algorithm that attempts to find the node with the lowest load, giving a bit of extra weight to the local
 * node (since all things being equal, it is more efficient not to distribute at all).
 *
 */
public class JGroupsDistributedForkJoinPool extends InfinispanDistributedForkJoinPool {
    public JGroupsDistributedForkJoinPool(Cache cache, int parallelism, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        super(parallelism, defaultForkJoinWorkerThreadFactory, uncaughtExceptionHandler, false, cache);

        destinationAddressStrategy = new SimpleDestinationAddressStrategy(cache.getAdvancedCache()); //TODO: let us override this

        try {
//            channel = new JChannel(configurationFileName);
            JGroupsTransport transport = (JGroupsTransport) cache.getAdvancedCache().getRpcManager().getTransport();
            channel = transport.getChannel();
            // Hack alert: we're actually co-opting the JGroupsTransport and setting a MessageListener, taking advantage of the
            // fact that (currently) there is no MessageListener set.
            transport.getCommandAwareRpcDispatcher().setMessageListener(new Listener());
//            channel.setReceiver(new Listener());

            // start listening.
            // Normal Executors have no separate "start" method; they start on
            // construction. So we have to do the same.
            start();
        } catch (Exception e) {
            log.error("Error co-opting JGroups channel used by the cache " + cache + ": ", e);
            throw rewrapAsCacheException(e);
        }
        log.info("Started");
    }


    public JGroupsDistributedForkJoinPool(Cache cache, int parallelism) {
        this(cache, parallelism, null);
    }

    DestinationAddressStrategy destinationAddressStrategy;

    // the JGroups channel
    private final Channel channel;

    /**
     * You don't have to explicitly call start() on a JGroupsDistributedForkJoinPool. We try to mimic the behavior of
     * other implementations of ExecutorService which start immediately on construction. So we do, too.
     */
    protected void start() {
        try {
            // connect to our JGroups cluster
//            channel.connect(groupName);
//            log.debug("Physical addresses of my cluster: " + cache.getRpcManager().getTransport().getPhysicalAddresses());
//            log.debug("My address, according to infinispan: " + cache.getRpcManager().getAddress());

        } catch (Exception e) {
            log.error("Error connecting to group " + //groupName +
                    ". This means that work will not be distributed. Error was: ", e);
        }

//        scheduledThreadPool.scheduleAtFixedRate(new DistributedForkJoinQueueWatcher(this), 10, 10, TimeUnit.MILLISECONDS);
    }

    @Override
    protected <T> void addWork(DistributedFJTask<T> forkJoinTask) {
        // is this task cache-aware? If so, fetch the keys. If not, this will be an empty list.
        List<Object> keys = getAssociatedCacheKeys(forkJoinTask);

        // note that we go ahead and pick a list of addresses instead of a partition. That abstracts everything hash-related
        // into the strategy implementation.
        List<Address> destinationAddresses = destinationAddressStrategy.lookup(keys);
        if (destinationAddresses != null && destinationAddresses.size() == 1 && destinationAddresses.get(0).equals(myAddress)) {
            // shortcut; just submit
            submitDirectly(forkJoinTask);
            return;
        }

        List<org.jgroups.Address> destinationJgroupsAddresses = convertAddresses(destinationAddresses);
        // record this task in our internal map
        tasksIDistributed.put(forkJoinTask.getId(), new TaskAndSource(forkJoinTask, channel.getAddress(), destinationJgroupsAddresses));

        // now, send this task off to those servers.
        byte[] executeMsg;
        try {
            executeMsg = createExecuteMsg(forkJoinTask, destinationAddresses);
        } catch (Exception e) {
            throw rewrapAsCacheException(e);
        }

        if (destinationJgroupsAddresses != null && !destinationJgroupsAddresses.isEmpty()) {
            for (org.jgroups.Address destinationAddress : destinationJgroupsAddresses) {
                try {
                    //TODO: shortcut and don't send if it's local. handleExecuteMessage() directly?
                    channel.send(destinationAddress, executeMsg);
                } catch (Exception e) {
                    log.warn("Error sending message to '" + destinationAddress + "'; moving on to next target. Problem was: "
                            + e.getMessage());
                    //TODO: need to think about this a bit more. Do we want to continue in this scenario? Fail? Fail only if it happens for all addresses? If this is the first address, do we want to go ahead and make someone else primary? That risks double-execution if the message actually went through and we just don't know it....
                }
            }
        } else {
            // No addresses to send to?  execute locally. This shouldn't really happen,
            // but we'll handle it in a reasonable manner if it does.
            submitDirectly(forkJoinTask);
        }

    }

    public View getView() {
        return channel.getView();
    }

    /**
     * Associated cache keys, or an empty list if none.
     * @param forkJoinTask
     * @param <T>
     * @return any associated cache keys, or an empty list if none.
     */
    private <T> List<Object> getAssociatedCacheKeys(ForkJoinTask<T> forkJoinTask) {
        List<Object> keys;
        if (forkJoinTask instanceof KeyAwareDistributedFJTask<?>) {
            keys = ((KeyAwareDistributedFJTask<?>) forkJoinTask).keys();
        } else {
            keys = Collections.emptyList();
        }
        return keys;
    }

    private org.jgroups.Address getJGroupsAddress(Address destinationAddress) {
        if (!(destinationAddress instanceof JGroupsAddress)) {
            throw new IllegalArgumentException("Destination address '" + destinationAddress + "' isn't a JGroups address. " +
                    "Unfortunately, we require JGroups as a transport infrastructure, so we can only work with JGroups addresses.");
        }
        return ((JGroupsAddress) destinationAddress).getJGroupsAddress();
    }

    private byte[] createExecuteMsg(DistributedFJTask<?> forkJoinTask, List<Address> destinationAddresses) throws Exception {
        ExecuteMessage executeMsg = new ExecuteMessage(forkJoinTask, convertAddresses(destinationAddresses));
        return Util.objectToByteBuffer(executeMsg);
    }

    private List<org.jgroups.Address> convertAddresses(List<Address> destinationAddresses) {
        List<org.jgroups.Address> newList = new ArrayList<org.jgroups.Address>(destinationAddresses.size());
        for (Address destinationAddress : destinationAddresses) {
            newList.add(getJGroupsAddress(destinationAddress));
        }
        return newList;
    }


    @Override
    protected <T> void notifyListenersOfNewTask(ForkJoinTask<T> task) {
        // No-op. This is a push-based algorithm, and there are no listeners.
    }


    public interface DestinationAddressStrategy {
        /**
         * Returns the list of addresses we should send to. The first address in this list will be the primary, and the
         * rest will be backup nodes in case the primary fails.
         *
         * @param keys
         * @return
         */
        public List<Address> lookup(List<Object> keys);
    }

    public static class LoadBasedDestinationAddressStrategy {
        //TODO: implement me.
    }

    public static class SimpleDestinationAddressStrategy implements DestinationAddressStrategy {
        final AdvancedCache cache;
        final boolean distributedMode;

        int lastTarget = 0;

        public SimpleDestinationAddressStrategy(AdvancedCache cache) {
            this.cache = cache;
            distributedMode = cache.getCacheConfiguration().clustering().cacheMode().isDistributed();
        }

        @Override
        public List<Address> lookup(List<Object> keys) {
            /* If a task is created and it is going to process data that is in the cache, then we'll use the same consistent hash
             * function that the cache is using. That way, the task should be executed on the same node that contains the cached
             * data. If a cache is distributed in such a way that multiple nodes contain the same data, this simple implementation
             * will just distribute round-robin.
             */
            if (keys != null && keys.size() > 0 && distributedMode /*as opposed to replicated*/) {
                DistributionManager distributionManager = cache.getDistributionManager();
                ConsistentHash hash = distributionManager.getConsistentHash();
                return hash.locateOwners(keys.get(0));
            } else {
                // we're either replicated mode (everyone has everything), or we have no keys (so the task isn't using cached data).
                // in either case, target this anywhere.

                // if we don't have a list of addresses to pick from, return our address.
                if (cache.getDistributionManager() == null
                        || cache.getDistributionManager().getConsistentHash() == null
                        || cache.getDistributionManager().getConsistentHash().getMembers() == null) {
                    return Collections.singletonList(getOurAddress(cache));
                }

                List<Address> addresses = cache.getDistributionManager().getConsistentHash().getMembers();

                // now just rotate, round-robin.
                // we're configured to send to at least #numOwners different nodes.
                int numOwners = Math.min(cache.getDistributionManager().getConsistentHash().getNumOwners(), addresses.size());

                int currLastTarget = lastTarget;
                // go ahead and advance lastTarget up front, to make it slightly less likely that we send to the same
                // more than once at the same time. If we do, though, it's not catastrophic.
                lastTarget = (lastTarget + numOwners) % addresses.size();

                List<Address> returnList = new ArrayList<Address>(numOwners);
                for (int i = 0; i < numOwners; i++) {
                    returnList.add(addresses.get((currLastTarget + i) % addresses.size()));
                }
                return returnList;
            }
        }
    }

    // have an abstract "onWork"-type method here:
    /*
    if we're using jgroups, then a JGroups onMessage will call onWork(work).
    if we're using message queues, then the message queue listener will pull when our load is low enough, or when our
     pool is empty. or something like that. Or perhaps just always pull.

     */


    /// our listener //
    private class Listener extends ReceiverAdapter {

        View view = null;

        /**
         * JGroups will call this when group membership changes.
         * @param newView
         */
        @Override
        public void viewAccepted(View newView) {
            handleMembershipChange(newView);
        }

        /**
         * Our main method; JGroups will call this when a message comes in over our channel.
         * @param message
         */
        public void receive(Message message) {
            FJMessage msg = extractMessage(message);
            if (msg instanceof ExecuteMessage) {
                handleExecuteMessage((ExecuteMessage) msg, message);
            } else if (msg instanceof ResultMessage) {
                handleResultMessage((ResultMessage) msg, message);
            } else if (msg instanceof RemoveMessage) {
                handleRemoveMessage((RemoveMessage) msg, message);
            }
        }

        /**
         * Helper method to deserialize our message. We assume anything coming in is an instance of FJMessage.
         * @param message
         * @return
         */
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

        /**
         * A "view" is JGroups's term for a snapshot of which nodes are connected to
         * this cluster. This method is called when our cluster membership listener
         * has detected a change in the cluster. If we need to do something in
         * particular when cluster membership changes, this is a good place to put
         * it.
         *
         * @param view
         */
        protected void handleMembershipChange(View view) {
            if (log.isDebugEnabled()) {
                log.debug("Cluster membership changed. \nPreviously: " + this.view + ". \nNow:        " + view);
            }
            List<org.jgroups.Address> leftMembers = Util.leftMembers(this.view, view);
            handleLeftMembers(leftMembers);
            this.view = view;
        }

        /**
         * When a node leaves the cluster, we need to look to see if they were
         * working on any tasks that we care about.
         * There are two reasons we might care about them:
         * <ul>
         *    <li>they were working on a task that we are tracking. In that case, we
         *    need to determine if we are the next live server in line, and if so,
         *    execute it.
         *    <li>they were working on a task that we submitted, and now none of the
         *    servers that were candidates to execute the task are still alive. In
         *    that case, resubmit the task.
         * </ul>
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
        private void handleLeftMembers(List<org.jgroups.Address> leftMembers) {
            if (leftMembers != null) {
                for (org.jgroups.Address member : leftMembers) {
                    Set<TaskAndSource> membersTasks = getTasksExecutedByMember(member);
//                    Set<DistributedFJTask<?>> membersTasks = getTasksExecutedByMember(member);
                    for (TaskAndSource taskAndSrc : membersTasks) {
                        if (iAmNextLivingMember(taskAndSrc.executors)) {
                            if (log.isDebugEnabled()) {
                                log.debug("Detected server failure for " + member.toString() + ":  I am the next living member for task " + taskAndSrc.toString() + ". Submitting.");
                            }
                            cleanupTaskMaps(taskAndSrc.task.getId(), member);
                            executeDistributableTask(taskAndSrc.task, taskAndSrc.src);
                        } else if (myOrphanTask(taskAndSrc)) { // the task was submitted by me all members originally assigned to it are dead
                            // resubmit the task.
                            submit(taskAndSrc.task);
                        }
                    }
                }
            }
            //TODO: what do we do when the submitter of a job that we are working on has died?
        }


        /**
         * Returns true if the task was submitted by me all members originally assigned to it are dead
         */
        private boolean myOrphanTask(TaskAndSource taskAndSrc) {
            if (tasksIDistributed.containsKey(taskAndSrc.task.getId())) {
                return false; // this wasn't my task.
            }
            View v = view;
            if (v != null) {
                for (org.jgroups.Address address : taskAndSrc.executors) {
                    if (v.containsMember(address)) {
                        return false; // this member is in the current view, so all members must not be dead.
                    }
                }
            }
            return true; // if we got here, then it was our task, and all the members are dead.
        }
    }


    private org.jgroups.Address nextLivingMember(List<org.jgroups.Address> addresses) {
        View v = channel.getView();
        if (v != null) {
            for (org.jgroups.Address address : addresses) {
                if (v.containsMember(address)) {
                    // this member is living
                    return address;
                }
            }
        }
        return null;
    }

    private boolean iAmNextLivingMember(List<org.jgroups.Address> addresses) {
        org.jgroups.Address nextLivingMember = nextLivingMember(addresses);
        return nextLivingMember != null && nextLivingMember.equals(channel.getAddress());
    }

    private Set<TaskAndSource> getTasksExecutedByMember(org.jgroups.Address member) {
        Set<TaskAndSource> set = executorToTaskMap.get(member);
        if (set == null) {
            return Collections.emptySet();
        } else {
            return set;
        }
    }

    ConcurrentMap<String, TaskAndSource> tasksIDistributed = CollectionFactory.makeConcurrentMap();
    ConcurrentMap<String, TaskAndSource> tasksDistributedToMe = CollectionFactory.makeConcurrentMap();
    ConcurrentMap<org.jgroups.Address, Set<TaskAndSource>> executorToTaskMap = CollectionFactory.makeConcurrentMap();


    public static class TaskAndSource {
        DistributedFJTask task;
        org.jgroups.Address src;
        List<org.jgroups.Address> executors;

        public TaskAndSource(DistributedFJTask task, org.jgroups.Address src, List<org.jgroups.Address> executors) {
            this.task = task;
            this.src = src;
            this.executors = executors;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TaskAndSource)) return false;

            TaskAndSource that = (TaskAndSource) o;

            if (!executors.equals(that.executors)) return false;
            if (!src.equals(that.src)) return false;
            if (!task.equals(that.task)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = task.hashCode();
            result = 31 * result + src.hashCode();
            result = 31 * result + executors.hashCode();
            return result;
        }
    }

    private void cleanupTaskMaps(String taskId, org.jgroups.Address address) {
        tasksIDistributed.remove(taskId);
        tasksDistributedToMe.remove(taskId);
        executorToTaskMap.remove(address);
    }

    private void handleExecuteMessage(ExecuteMessage msg, Message message) {
        DistributedFJTask<?> task = msg.getTask();
        TaskAndSource taskAndSource = new TaskAndSource(task, message.getSrc(), msg.getAddresses());
        tasksDistributedToMe.put(task.getId(), taskAndSource);
        org.jgroups.Address executor = nextLivingMember(msg.getAddresses());
        if (executor != null) {
            executorToTaskMap.putIfAbsent(executor, new HashSet<TaskAndSource>());
            executorToTaskMap.get(executor).add(taskAndSource);
            // if I'm the next living member, execute it.
            if (executor.equals(channel.getAddress())) {
                executeDistributableTask(task, message.getSrc());
            }
            // otherwise, just store it away. If the designated executor dies, we'll revisit whether we should execute this.
        }
    }

    private <T> void executeDistributableTask(final DistributedFJTask<T> task, final org.jgroups.Address src) {
        task.attachListener(new ResponseSender<T>(task, src));
        this.submitDirectly(task);
    }

    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "unchecked"})
    private void handleResultMessage(ResultMessage msg, Message message) {
        if (log.isTraceEnabled()) {
            log.trace("Got a response for task " + msg.getTaskId() + " back from server "
                    + message.getSrc().toString());
        }
        // look up our local version of this task and set the response.
        DistributedFJTask ourTask = getDistributedTaskById(msg.getTaskId(), message.getSrc());
        if (ourTask == null) {
            log.error("Cannot process result of task " + msg.getTaskId() + ": task not found!");
            return;
        }
        // clean up: remove this task from our maps.
        cleanupTaskMaps(msg.getTaskId(), message.getSrc());
        // msg has a task, but it could have a throwable, too, in case an
        // error was thrown while executing this task.
        if (msg.getThrowable() != null) {
            ourTask.completeExceptionally(msg.getThrowable());
        } else {
            ourTask.complete(msg.getResult());
        }
    }

    private DistributedFJTask getDistributedTaskById(String taskId, org.jgroups.Address src) {
        TaskAndSource taskAndSource = tasksIDistributed.get(taskId);
        if (taskAndSource != null) {
            return taskAndSource.task;
        }
        return null;
    }

    private void handleRemoveMessage(RemoveMessage msg, Message message) {
        cleanupTaskMaps(msg.getTaskId(), message.getSrc());
    }


    private class ResponseSender<T> implements FutureListener<T> {
        private final DistributedFJTask<?> task;
        private final org.jgroups.Address src;

        public ResponseSender(DistributedFJTask<?> task, org.jgroups.Address src) {
            this.src = src;
            this.task = task;
        }

        @Override
        public void futureDone(Future<T> future) {
            try {
                Object result = future.get();
                send(new ResultMessage(task.getId(), result));
            } catch (Throwable e) {
                send(new ResultMessage(task.getId(), e));
            }
        }

        private void send(FJMessage msg) {
            try {
                channel.send(src, msg);
            } catch (Exception e) {
                log.error("Error sending result of task " + task + " to " + src + ". This isn't good.");
                // TODO: retry, with backoff
            }
        }

    }


    /// Messages ///

    /**
     * marker superclass.
     */
    public static abstract class FJMessage implements Streamable {

    }

    public static class ExecuteMessage extends FJMessage {
        DistributedFJTask<?> task;
        List<org.jgroups.Address> addresses;

        // no-arg constructor for serialization purposes.
        public ExecuteMessage() {
        }

        public ExecuteMessage(DistributedFJTask<?> task, List<org.jgroups.Address> addresses) {
            this.task = task;
            this.addresses = addresses;
        }

        public DistributedFJTask<?> getTask() {
            return task;
        }

        public List<org.jgroups.Address> getAddresses() {
            return addresses;
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            writeList(addresses, out);
            Util.writeObject(task, out);
        }


        @Override
        public void readFrom(DataInput in) throws Exception {
            addresses = (List<org.jgroups.Address>) readList(in);
            task = (DistributedFJTask<?>) Util.readObject(in);
        }

        /**
         * writes the list prepended by the length. Restricted to collections with no more than 32,767 elements, since
         * it uses a Java short (16-bit signed integer) to represent it.
         * @param list
         * @param out
         * @throws Exception
         */
        private void writeList(List<?> list, DataOutput out) throws Exception {
            if (list == null) {
                out.writeShort(-1);
                return;
            }
            out.writeShort(list.size());
            for (Object o : list) {
                Util.writeObject(o, out);
            }
        }

        public static Collection readList(DataInput in) throws Exception {
            short length = in.readShort();
            if (length < 0) {
                return null;
            }
            List<Object> list = new ArrayList<Object>(length);
            for (int i = 0; i < length; i++) {
                list.add(Util.readObject(in));
            }
            return list;
        }

    }

    public static class ResultMessage extends FJMessage {
        private String taskId;
        private Object result;
        private Throwable throwable;

        public ResultMessage(String taskId, Object result) {
            this.taskId = taskId;
            this.result = result;
        }

        public ResultMessage(String taskId, Throwable t) {
            this.taskId = taskId;
            this.throwable = t;
        }

        // no-arg constructor for serialization purposes.
        public ResultMessage() {
        }

        public String getTaskId() {
            return taskId;
        }

        public Object getResult() {
            return result;
        }

        public Throwable getThrowable() {
            return throwable;
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            Util.writeString(taskId, out);
            Util.writeObject(result, out);
            Util.writeObject(throwable, out);
        }

        @Override
        public void readFrom(DataInput in) throws Exception {
            taskId = Util.readString(in);
            result = Util.readObject(in);
            throwable = (Throwable) Util.readObject(in);
        }
    }

    public static class RemoveMessage extends FJMessage {
        String taskId;

        // no-arg constructor for serialization purposes.
        public RemoveMessage() {
        }

        public RemoveMessage(String taskId) {
            this.taskId = taskId;
        }

        public String getTaskId() {
            return taskId;
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

}

