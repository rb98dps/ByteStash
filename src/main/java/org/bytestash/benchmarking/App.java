package org.bytestash.benchmarking;

import org.bytestash.cache.CacheNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.BadAttributeValueExpException;
import java.time.Instant;
import java.util.Random;

/**
 * Hello world!
 */
public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static long testCacheNode(int iteration, int times, long gap, int capacity, boolean printCache, boolean printValues) throws BadAttributeValueExpException, InterruptedException {
        int count = 0;
        long average = 0;
        while (count < iteration) {
            CacheNode<Object> cacheNode = new CacheNode<>(capacity, 0);
            Instant instant = Instant.now();
            for (int i = 0; i < times; i++) {
                Thread.sleep(gap);
                Random random = new Random();
                int x = random.nextInt(3);
                switch (x) {
                    case 0 -> {
                        if (printValues) {
                            logger.debug("Put: {}", i);
                        }
                        cacheNode.put(String.valueOf(i), i);
                    }
                    case 1 -> {
                        int y = random.nextInt(i + 1);
                        if (printValues) {
                            logger.debug("Get:{}", y);
                            logger.debug("Found Value:{}", cacheNode.get(String.valueOf(y)));
                        }

                    }
                    case 2 -> {
                        int y = random.nextInt(i + 1);
                        if (printValues) {
                            logger.debug("Remove:{}", y);
                            logger.debug("Removed: {}", cacheNode.remove(String.valueOf(y)));
                        }
                    }
                }
                cacheNode.checkCacheAndRegion();
                if (printCache)
                    cacheNode.printCacheState();

            }
            long timeTaken = Instant.now().toEpochMilli() - instant.toEpochMilli();
            average += timeTaken;
            logger.debug("TimeTaken for Iteration {}", timeTaken);
            count++;

        }
        logger.debug("Average for Iteration {}", average / iteration);
        return average / iteration;
    }

    public static long testPutInCacheNode(int iteration, int times, long gap, int capacity, boolean printCache, boolean printValues) throws BadAttributeValueExpException, InterruptedException {
        int count = 0;
        long average = 0;
        while (count < iteration) {
            CacheNode<Object> cacheNode = new CacheNode<>(capacity, 0);
            Instant instant = Instant.now();
            for (int i = 0; i < times; i++) {
                Thread.sleep(gap);
                if (printValues) {
                    logger.debug("Put: {}", i);
                }
                cacheNode.put(String.valueOf(i), i);

            }
            long timeTaken = Instant.now().toEpochMilli() - instant.toEpochMilli();
            average += timeTaken;
            logger.debug("TimeTaken for Iteration {}", timeTaken);
            count++;

        }
        logger.debug("Average for Iteration {}", average / iteration);
        return average / iteration;

    }


    public static CacheNode<Integer> getFilledCache(int times, long gap, int capacity, boolean printCache, boolean printValues) throws BadAttributeValueExpException, InterruptedException {
        CacheNode<Integer> cacheNode = new CacheNode<>(capacity, 0);
        for (int i = 0; i < times; i++) {
            Thread.sleep(gap);
            if (printValues) {
                logger.debug("Put: {}", i);
            }
            cacheNode.put(String.valueOf(i), i);
        }
        return cacheNode;
    }

    public static long testGetInCacheNode(CacheNode<?> cacheNode, int iteration, int times, long gap, int capacity, boolean printCache, boolean printValues) throws BadAttributeValueExpException, InterruptedException {
        int count = 0;
        long average = 0;
        while (count < iteration) {
            Instant instant = Instant.now();
            for (int i = 0; i < times; i++) {
                Thread.sleep(gap);
                Random random = new Random();
                int y = random.nextInt(i + 1);
                if (printValues) {
                    logger.debug("Get:{}", y);
                    logger.debug("Found Value:{}", cacheNode.get(String.valueOf(y)));
                }
            }
            long timeTaken = Instant.now().toEpochMilli() - instant.toEpochMilli();
            average += timeTaken;
            logger.debug("TimeTaken for Iteration {}", timeTaken);
            count++;

        }
        logger.debug("Average for Iteration {}", average / iteration);
        return average / iteration;

    }


    public static long testGetAndPutInCacheNode(int iteration, int times, long gap, int capacity, boolean printCache, boolean printValues) throws BadAttributeValueExpException, InterruptedException {
        int count = 0;
        long average = 0;
        while (count < iteration) {
            CacheNode<Object> cacheNode = new CacheNode<>(capacity, 0);
            Instant instant = Instant.now();
            for (int i = 0; i < times; i++) {
                Thread.sleep(gap);
                Random random = new Random();
                int x = random.nextInt(2);
                switch (x) {
                    case 0 -> {
                        if (printValues) {
                            logger.debug("Put: {}", i);
                        }
                        cacheNode.put(String.valueOf(i), i);
                    }
                    case 1 -> {
                        int y = random.nextInt(i + 1);
                        if (printValues) {
                            logger.debug("Get:{}", y);
                            logger.debug("Found Value:{}", cacheNode.get(String.valueOf(y)));
                        }

                    }
                }
                cacheNode.checkCacheAndRegion();
                if (printCache)
                    cacheNode.printCacheState();

            }
            long timeTaken = Instant.now().toEpochMilli() - instant.toEpochMilli();
            average += timeTaken;
            logger.debug("TimeTaken for Iteration {}", timeTaken);
            count++;

        }
        logger.debug("Average for Iteration {}", average / iteration);
        return average / iteration;

    }


}
