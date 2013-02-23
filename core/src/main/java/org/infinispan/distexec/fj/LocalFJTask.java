package org.infinispan.distexec.fj;

import org.infinispan.util.concurrent.jdk8backported.RecursiveTask;

/**
 * For help in deciding whether you should use this or {@link DistributableTask}, see the javadoc in DistributableTask.
 *
 * This is the same as RecursiveTask, but it's here to insulate us from future repackaging of the underlying ForkJoin framework.
 *
 * @see DistributableTask
 * @see RecursiveTask
 */
public abstract class LocalFJTask<V> extends RecursiveTask<V> {

	private static final long serialVersionUID = -7954252157939657092L;

}
