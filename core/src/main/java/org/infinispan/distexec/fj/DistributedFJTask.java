package org.infinispan.distexec.fj;


import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.RunnableFuture;

import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.distexec.DistributedTaskExecutionPolicy;
import org.infinispan.distexec.DistributedTaskFailoverPolicy;
import org.infinispan.util.concurrent.jdk8backported.*;

/**
 * TODO: change this to an interface, and let the main implementor extend RecursiveTask?  Then we'd need to have a wrapper, though, right?
 * Class indicating that this particular RecursiveTask can be distributed.
 *
 * In essence, saying a task can be distributed is effectively saying "this task is coarse-grained enough that it's worth sending to another machine."
 * If a task is very small, it is often not worth sending off to another server to process; the communication overhead is too high.
 *
 * Obviously, there's cases where that's not true. For example, if we had a ton of very fine-grained tasks, it would still be worth it
 * to distribute some of those tasks; the overhead in distribution may well be less than the amount of time it would take one machine
 * to execute all tasks. And it could be possible to detect those situations and automatically distribute tasks accordingly: we can estimate our queue length,
 * for example.
 * But instead we are relying on developers to design their tasks such that they are broken down from larger parts into smaller,
 * and furthermore that they mark those larger tasks as "distributable" for us. Further optimization is TBD.
 *
 * <h2>Deciding whether to use DistributableTask or LocalTask</h2>
 *
 * Sometimes the decision for when to make a task a DistributableTask vs. a LocalTask is fairly clear. Sometimes it isn't.
 * As a general heuristic, consider the amount of time it would take to serialize a task, send it to another server, and deserialize
 * it. Then add to that the time it would take to serialize the result, send that back to the this server, and deserialize it.
 *
 *
 * There are tradeoffs involved in deciding between DistributableTask and LocalTask.  For example:
 * <ul>
 *     <li>
 *         Obviously, if you use LocalTask, the task cannot be distributed, even if the current server is very backed up.
 *     </li>
 *     <li>
 *         If you use DistributedTask, but the task is very small, it could be that the time it takes to transfer the task
 *         to another server and then transfer the results back is higher than the time it would have taken to execute the task
 *         locally. Note that you can attempt to crudely predict this case algorithmically, if you really want, by estimating
 *         the queue size of the current node, and then using an educated guess for the execution time of those tasks. This
 *         is, at best, a very rough heuristic, and dependent on an accurate knowledge of what is currently running.
 *     </li>
 *     <li>
 *         Because the "distribution" portion of the ForkJoin framework is bolted on, making something a DistributableTask
 *         introduces some overhead even if the task is only distributed locally. This should be on the order of milliseconds,
 *         at most, but that could be significant if the tasks are large.
 *     </li>
 * </ul>
 * add some overhead.
 *
 * If you are algorithmically deciding whether a task should be a DistributableTask or a LocalTask, consider making it a
 * DistributableTask just in case.
 *
 * If in doubt, you could do something like the following from within your task, but <b>please note</b> that we have not
 * yet verified that this is actually worth it. Want to be the first and run some tests?
 *
   <code>
            if (ForkJoinTask.inForkJoinPool()) {
                 if (ForkJoinTask.getSurplusQueuedTaskCount() > 3) {
                     // create a DistributableTask
                 } else {
                     // create a LocalTask
                 }
             } else {
                 // we're not within a currently executing ForkJoinTask, so we can't check surplus queued task count.
                 // create a DistributableTask, just in case.
             }
   </code>
 *
 */
public abstract class DistributedFJTask<V> extends RecursiveTask<V> {

//    final private static Timer serializationTimer = Metrics.defaultRegistry().newTimer(DistributedFJTask.class, "serialization", TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);
//    final private static Timer deserializationTimer = Metrics.defaultRegistry().newTimer(DistributedFJTask.class, "deserialization", TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);
//    final private static Timer hydrationTimer = Metrics.defaultRegistry().newTimer(DistributedFJTask.class, "hydration", TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);
    protected String id = UUID.randomUUID().toString();

    // you still need to implement the "compute" method.

//    /**
//     * The default implementation here is quite na√Øve. You are highly encouraged to implement a more efficient method here.
//     *
//     * TODO: This can't really work, though. We need an accompanying deserializer that we can look up or reference on the receiving end, and of course that would need to be in a helper class (since this class is still serialized then)
//     *
//     * @return
//     */
//    public byte[] serialize() {
//        // naive implementation
//        return SerializationUtils.serialize(this);
//        // TODO: attempt to use Kryo and fall back on Java serialization if necessary.
//    }

    //TODO: put in a TaskSerializerFactory and a ValueSerializerFactory?  Default implementations use Java serialization?
    // But the factory itself would need to be accessible on the other side...
    // perhaps the bytes we send always includes a deserialization class name? Has to be able to be instantiated with a no-arg constructor,
    // maybe has to be threadsafe, and has to be accessible to the caller?
    // Or, just extend the Externalizable interface, and make the default implementation use Java serialization? Or make the default use Kryo,
    // and make people explicitly use Serializable if Kryo doesn't work for them?
    // Alternately, we can send an s-expression. Not sure if that would be better.

    public String getId() {
        return id;
    }

    @Override
    public ForkJoinTask<V> fork() {
        ForkJoinPool pool = ((ForkJoinWorkerThread)Thread.currentThread()).getPool();
        // make sure we're working in a DistributedForkJoinPool. If not, fork normally.
        if (pool instanceof AbstractDistributedForkJoinPool) {
            // need to go through the proper channels so distributable things go into their special deque.
            ((AbstractDistributedForkJoinPool)pool).submitDistributable(this);
        } else {
        	super.fork();
        }
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DistributedFJTask)) return false;

        DistributedFJTask that = (DistributedFJTask) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    /**
     * You're encouraged to override this with something more descriptive.
     * @return
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }



    /**
     * Returns a new {@code ForkJoinTask} that performs the {@code call}
     * method of the given {@code Callable} as its action, and returns
     * its result upon {@link #join}, translating any checked exceptions
     * encountered into {@code RuntimeException}.
     *
     * @param callable the callable action
     * @return the task
     */
    public static <T> DistributedFJTask<T> adapt(Callable<? extends T> callable) {
        return new AdaptedCallable<T>(callable);
    }

    /**
     * Adaptor for Callables
     */
    static class AdaptedCallable<T> extends DistributedFJTask<T> implements RunnableFuture<T> {
    	private static final long serialVersionUID = 9134233435355241000L;
        final Callable<? extends T> callable;
        T result;

        AdaptedCallable(Callable<? extends T> callable) {
            if (callable == null) throw new NullPointerException();
            this.callable = callable;
        }

        public final T compute() {
            try {
                return callable.call();
            } catch (Error err) {
                throw err;
            } catch (RuntimeException rex) {
                throw rex;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public final void run() {
        	invoke();
    	}

    }
}
