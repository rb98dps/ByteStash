package org.bytestash.taskhandler;

import org.bytestash.cache.CacheRegionType;
import org.bytestash.cache.Crawlable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

public class TaskQueueHandler {
    private static final Logger logger = LoggerFactory.getLogger(TaskQueueHandler.class);
    private Queue<NodeWork> queue;

    private final int MAX_SIZE;

    public TaskQueueHandler(int maxSize) {
        MAX_SIZE = maxSize;
        queue = new LinkedList<>();

    }

    public synchronized void addTask(Crawlable crawlable, CacheRegionType cacheRegion, int pos) {
        NodeWork work = new NodeWork(crawlable, cacheRegion, pos);
        if (MAX_SIZE >= queue.size()) {
            queue.add(work);
            logger.debug("Task added for Node {} at pos {}",crawlable,pos);
        }

    }

    public synchronized NodeWork removeTask() {
        if (!queue.isEmpty()) {
            return queue.poll();
        } else {
            return null;
        }
    }
}
