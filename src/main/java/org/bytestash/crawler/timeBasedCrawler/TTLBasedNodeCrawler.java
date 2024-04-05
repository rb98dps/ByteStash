package org.bytestash.crawler.timeBasedCrawler;

import lombok.ToString;
import org.bytestash.cache.CacheRegionType;
import org.bytestash.cache.Crawlable;
import org.bytestash.crawler.NodeCrawler;
import org.bytestash.evictionpolicy.TimeStampBasedEvictionInfo;
import org.bytestash.taskhandler.NodeWork;
import org.bytestash.taskhandler.TaskQueueHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
@ToString
public class TTLBasedNodeCrawler<T> implements NodeCrawler<T> {
    private static final Logger logger = LoggerFactory.getLogger(TTLBasedNodeCrawler.class);
    boolean busy;
    private final ScheduledExecutorService scheduler;
    org.bytestash.crawler.timeBasedCrawler.TTLBasedCrawler<T> TTLBasedCrawler;
    TaskQueueHandler taskQueueHandler;

    private final int index;

    public TTLBasedNodeCrawler(boolean busy, TTLBasedCrawler<T> TTLBasedCrawler, TaskQueueHandler taskQueueHandler, int index) {
        this.busy = busy;
        this.TTLBasedCrawler = TTLBasedCrawler;
        this.taskQueueHandler = taskQueueHandler;
        this.index = index;
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public synchronized void nodeCleanup(Crawlable crawlable, CacheRegionType cacheRegion, int pos) {
        logger.debug("Started cleaning of node {}, region {}", crawlable, cacheRegion);
        busy = true;
        try {
            TimeStampBasedEvictionInfo timeStampBasedRemovedInfo = (TimeStampBasedEvictionInfo) crawlable.removeItems(cacheRegion);
            TTLBasedCrawler.changeOldTimeStamp(pos, cacheRegion, timeStampBasedRemovedInfo.getOldestTimeStamp());
        } catch (Exception e){
            logger.debug("Error while cleaning Node: {}, Region: {} with error : {}",crawlable,cacheRegion,e,e);
        } finally {
            busy = false;
        }
        logger.debug("Finished cleaning of node {}, region {}", crawlable, cacheRegion);
    }

    public void getTask() {
        if (!busy) {
            NodeWork nodeWork = taskQueueHandler.removeTask();
            if(nodeWork != null) {
                logger.debug("Task taken by {} for work {}", index, nodeWork);
                nodeCleanup(nodeWork.getCrawlable(), nodeWork.getCacheRegion(), nodeWork.getPos());
            }
        }
    }


    public void startTaskScheduler() {
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
