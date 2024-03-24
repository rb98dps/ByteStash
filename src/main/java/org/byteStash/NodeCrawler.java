package org.byteStash;

import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
@ToString
public class NodeCrawler<T> {
    private static final Logger logger = LoggerFactory.getLogger(NodeCrawler.class);
    boolean busy;
    private final ScheduledExecutorService scheduler;
    Crawler<T> crawler;
    TaskQueueHandler<T> taskQueueHandler;

    private final int index;

    public NodeCrawler(boolean busy, Crawler<T> crawler, TaskQueueHandler<T> taskQueueHandler, int index) {
        this.busy = busy;
        this.crawler = crawler;
        this.taskQueueHandler = taskQueueHandler;
        this.index = index;
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public synchronized void nodeCleanup(CacheNode<T> node, CacheRegionType cacheRegion, int pos) {
        logger.debug("Started cleaning of node {}, region {}", node, cacheRegion);
        busy = true;
        try {
            Timestamp timestamp = node.removeAllExpiredItems(cacheRegion);
            crawler.changeOldTimeStamp(pos, cacheRegion, timestamp);
        } catch (Exception e){
            logger.debug("Error while cleaning Node: {}, Region: {} with error : {}",node,cacheRegion,e,e);
        } finally {
            busy = false;
        }
        logger.debug("Finished cleaning of node {}, region {}", node, cacheRegion);
    }

    public void getTask() {
        if (!busy) {
            NodeWork<T> nodeWork = taskQueueHandler.removeTask();
            if(nodeWork != null) {
                logger.debug("Task taken by {} for work {}", index, nodeWork);
                nodeCleanup(nodeWork.getCacheNode(), nodeWork.getCacheRegion(), nodeWork.getPos());
                nodeWork.getCacheNode().printCacheState();
            }
        }
    }


    public void startTaskScheduler() {
        // Schedule the task to run every 1 minute

        scheduler.scheduleAtFixedRate(this::getTask, 0, 1, TimeUnit.SECONDS);
    }

    public void stopScheduler() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

}
