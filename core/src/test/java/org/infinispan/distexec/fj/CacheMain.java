/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.distexec.fj;

import org.apache.log4j.lf5.util.StreamUtils;
import org.infinispan.Cache;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.concurrent.jdk8backported.ForkJoinTask;
import org.jgroups.conf.ConfiguratorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Argument handling here is particularly ugly, but it's quick n' dirty n'
 * works.
 *
 * -Djgroups.bind_addr=192.168.5.2 and
 * -Djgroups.tcpping.initial_hosts=192.168.5.2[7800]".
 *
 */
public class CacheMain {

    private static final String MEM = "256m";

    /**
     * hooray, it's like a struct, only in Java! :)
     */
    public static class AskerArguments {

        public String waitTimeInMs;
        public String threshold;
        public String count;
        public String parallelism;
        public String filename;

    }

    /**
     * Simple class for testing.
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println(getUsage());
            System.exit(1);
        }
        try {
            String op = args[0];
            if (op.equalsIgnoreCase("listen")) {
                if (args.length == 2) {
                    startListener(null, args[1]);
                } else if (args.length == 3) {
                    startListener(args[1], args[2]);
                } else {
                    System.err.println(getUsage());
                    System.exit(1);
                }
            } else if (op.equalsIgnoreCase("ask")) {
                AskerArguments a = loadArguments(args);
                startAsker(a.filename, a.parallelism, a.count, a.threshold, a.waitTimeInMs);
                // after the asker returns, we exit.
                System.exit(0);
            } else if (op.equalsIgnoreCase("both")) {
                int numListeners = Integer.parseInt(args[1]);
                AskerArguments a = loadArguments(Arrays.copyOfRange(args, 1, 10));
                String separator = System.getProperty("file.separator");
                String classpath = System.getProperty("java.class.path");
                String path = System.getProperty("java.home") + separator + "bin" + separator + "java";
                // start listeners
                List<Process> listeners = new ArrayList<Process>(numListeners);
                for (int i = 0; i < numListeners; i++) {
                    System.out.println("starting listener " + (i + 1));
                    List<String> argsList = new ArrayList<String>();
                    argsList.add(path);
                    argsList.add("-cp");
                    argsList.add(classpath);
                    argsList.add("-Xmx" + MEM);
                    argsList.add(CacheMain.class.getName());
                    argsList.add("listen");
                    if (a.filename != null)
                        argsList.add(a.filename);
                    argsList.add(a.parallelism);
                    ProcessBuilder processBuilder = new ProcessBuilder(argsList);
                    Process process = processBuilder.start();
                    listeners.add(process);
                }

                // start the asker
                System.out.println("starting asker");
                List<String> argsList = new ArrayList<String>();
                argsList.add(path);
                argsList.add("-cp");
                argsList.add(classpath);
                argsList.add("-Xmx" + MEM);
                argsList.add(CacheMain.class.getName());
                argsList.add("ask");
                if (a.filename != null)
                    argsList.add(a.filename);
                argsList.add(a.parallelism);
                argsList.add(a.count);
                argsList.add(a.threshold);
                argsList.add(a.waitTimeInMs);
                ProcessBuilder processBuilder = new ProcessBuilder(argsList);
                processBuilder.redirectErrorStream();
                Process asker = processBuilder.start();

                // wait for it to complete.
                asker.waitFor();

                // see the logs
                StreamUtils.copy(asker.getErrorStream(), System.out);
                StreamUtils.copy(asker.getInputStream(), System.out);

                // clean up
                System.out.println("Finished, shutting down");
                for (Process process : listeners) {
                    process.destroy();
                }
                asker.destroy();
                System.exit(0);
            } else {
                System.err.println(getUsage());
                System.exit(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    private static AskerArguments loadArguments(String[] args) {
        AskerArguments a = new AskerArguments();
        // a.filename, a.parallelism, a.count, a.threshold, a.waitTimeInMs);
        if (args.length == 6) {
            a.filename = args[1];
            a.parallelism = args[2];
            a.count = args[3];
            a.threshold = args[4];
            a.waitTimeInMs = args[5];
        } else {
            a.filename = null;
            a.parallelism = args[1];
            a.count = args[2];
            a.threshold = args[3];
            a.waitTimeInMs = args[4];
        }
        return a;
    }

    private static void startAsker(String filename, String parallelismString, String countString,
                                   String thresholdString, String waitTimeInMsString) throws Exception {

        int waitTime = Integer.parseInt(waitTimeInMsString);
        int threshold = Integer.parseInt(thresholdString);
        int parallelism = Integer.parseInt(parallelismString);
        int count = Integer.parseInt(countString);

        Cache cache = createCache(filename);
        TaskCachingDistributedForkJoinPool fjPool = new TaskCachingDistributedForkJoinPool(cache,
                Integer.parseInt(parallelismString));
        DistributedSumTask sum = new DistributedSumTask(0, Integer.parseInt(countString),
                Integer.parseInt(thresholdString), Integer.parseInt(waitTimeInMsString));
        long start = System.currentTimeMillis();
        startEfficiencyLogger();
        System.out.println("Submitting fork join job counting to " + countString + " with a sum threshold of "
                + thresholdString + " and each task waiting " + waitTimeInMsString + "ms");
        ForkJoinTask<Long> result = fjPool.submit(sum);
        long sumResult = 0;
        try {
            sumResult = result.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        long elapsedTime = (System.currentTimeMillis() - start);
        System.out.println("Got result " + sumResult + " in " + elapsedTime + "ms");
        TaskCachingDistributedForkJoinPoolTest
                .printInterestingOutput(Integer.parseInt(countString), System.out, fjPool);
        int numberOfNodes = cache.getCacheManager().getMembers().size();
        System.out.println("There were " + numberOfNodes + " members in the cluster");
        double numTasks = numberOfTasks(count, threshold);
        System.out.println(String.format("There were %.0f tasks total", numTasks));
        double expectedTime = (waitTime + 1) * numTasks / (double) (parallelism * numberOfNodes);
        System.out.println(String.format("Ideal runtime is %.2f'ms', but it was %d. Our efficiency was %.2f'%'",
                expectedTime, elapsedTime, (expectedTime / elapsedTime)));
        fjPool.shutdownNow();
        cache.getCacheManager().stop();
    }

    private static double numberOfTasks(int count, int threshold) {
        int numLevels = 0;
        int c = count;
        while (c > threshold) {
            numLevels++;
            c = c / 2;
        }
        return Math.pow(2, numLevels) - 1;

    }

    private static void startListener(String filename, String parallelismString) throws Exception {
        Cache cache = createCache(filename);
        TaskCachingDistributedForkJoinPool fjPool = new TaskCachingDistributedForkJoinPool(cache,
                Integer.parseInt(parallelismString));
        System.out.println("Started listener");
        startEfficiencyLogger();
        // it's going to stay alive.
    }

    private static void startEfficiencyLogger() {
        Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(new FJThreadMonitor(5000), 1000, 100,
                TimeUnit.MILLISECONDS);
    }

    protected static Cache createCache(String filename) throws Exception {
        Cache cache;
        if (filename != null) {
            cache = new DefaultCacheManager(filename).getCache();
        } else {
            GlobalConfiguration globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder()
                    .clusteredDefault().transport()
                    .addProperty(JGroupsTransport.CONFIGURATION_FILE, "stacks/tcpping.xml").build();

            cache = new DefaultCacheManager(globalConfiguration, true).getCache();

        }
        return cache;
    }

    private static String getJGroupsConfig() throws Exception {
        // return
        // ConfiguratorFactory.getStackConfigurator("tcp-nio.xml").toString();
        return ConfiguratorFactory.getStackConfigurator("stacks/tcpping.xml").toString();
    }

    protected static String getUsage() {
        return "Usage: \n"
                + "listener:   Main listen [infinispan configuration file] <parallelism> \n"
                + "asker:      Main ask [infinispan configuration file] <parallelism> <count> <threshold> <wait time in ms>  \n"
                + "both:       Main both <number of listeners> [infinispan configuration file] <parallelism> <count> <threshold> <wait time in ms>  \n"
                + "\n"
                + "you may also want some system properties for jgroups, e.g.:\n"
                + "     -Djgroups.bind_addr=192.168.5.2 -Djgroups.bind_port=7800 -Djgroups.tcpping.initial_hosts=192.168.5.2[7800]\n"
                + "\n";
    }

}
