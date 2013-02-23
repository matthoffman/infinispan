package org.infinispan.distexec.fj;

import java.util.concurrent.ExecutionException;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.concurrent.jdk8backported.ForkJoinTask;

/**
 * Argument handling here is particularly ugly, but it's quick n' dirty n' works.
 *
 *      -Djgroups.bind_addr=192.168.5.2 and -Djgroups.tcpping.initial_hosts=192.168.5.2[7800]".
 *
 *
 */
public class Main {



	/**
	 * Simple class for testing.
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println(getUsage());
			System.exit(1);
		}

		String op = args[0];
		if (op.equalsIgnoreCase("listen")) {
			startListener(args[1], args[2]);
		} else if (op.equalsIgnoreCase("ask") && args.length == 6) {
			startAsker(args[1], args[2], args[3], args[4], args[5]);
		} else {
			System.err.println(getUsage());
			System.exit(1);
		}

	}

	private static void startAsker(String filename, String parallelismString, String countString, String thresholdString, String waitTimeInMsString) {
		Cache cache = new DefaultCacheManager(true).getCache();
		DefaultDistributedForkJoinPool fjPool = new DefaultDistributedForkJoinPool(cache, filename, "test group", Integer.parseInt(parallelismString));
		DistributedSumTask sum = new DistributedSumTask(0, Integer.parseInt(countString), Integer.parseInt(thresholdString), Integer.parseInt(waitTimeInMsString));
		long start = System.currentTimeMillis();
		System.out.println("Submitting fork join job counting to "+ countString+" with a sum threshold of "+ thresholdString+" and each task waiting "+ waitTimeInMsString+"ms");
		ForkJoinTask<Long> result = fjPool.submit(sum);
		long sumResult = 0;
		try {
			sumResult = result.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		System.out.println("Got result "+ sumResult+" in "+ (System.currentTimeMillis()-start)+"ms");

	}

	private static void startListener(String filename, String parallelismString) {
		Cache cache = new DefaultCacheManager(true).getCache();
		DefaultDistributedForkJoinPool fjPool = new DefaultDistributedForkJoinPool(cache, filename, "test group", Integer.parseInt(parallelismString));
		// it's going to stay alive.
	}

	protected static String getUsage() {
		return "Usage: \n" +
				"listener:   Main listen <jgroups file> <parallelism> \n" +
				"asker:      Main ask <jgroups file> <parallelism> <count> <threshold> <wait time in ms>  \n" +
				"\n" +
				"you may also want some system properties for jgroups, e.g.:\n" +
				"     -Djgroups.bind_addr=192.168.5.2 -Djgroups.bind_port=7800 -Djgroups.tcpping.initial_hosts=192.168.5.2[7800]\n"+
				"\n";
	}

}
