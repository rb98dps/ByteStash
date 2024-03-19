package org.byteStash;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

public class TaskQueueHandler<T> {
    private static final Logger logger = LoggerFactory.getLogger(TaskQueueHandler.class);
    private Queue<NodeWork<T>> queue;

    private final int MAX_SIZE;

    TaskQueueHandler(int maxSize) {
        MAX_SIZE = maxSize;
        queue = new LinkedList<>();

    }

    public synchronized void addTask(CacheNode<T> node, CacheRegion cacheRegion, int pos) {
        NodeWork<T> work = new NodeWork<>(node, cacheRegion, pos);
        if (MAX_SIZE >= queue.size()) {
            queue.add(work);
            logger.debug("Task added for Node {} at pos {}",node,pos);
        }

    }

    public synchronized NodeWork<T> removeTask() {
        if (!queue.isEmpty()) {
            return queue.poll();
        } else {
            return null;
        }
    }
}
