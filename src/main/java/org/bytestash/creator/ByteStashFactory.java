package org.bytestash.creator;

import lombok.Builder;
import org.bytestash.crawler.CrawlerType;
import org.bytestash.taskhandler.TaskQueueHandler;


public class ByteStashFactory<T> implements CacheManagerFactory<Object,T>{

    private static final long MIN_CAPACITY = 10L;
    private static final long MAX_CAPACITY = 1000000L;
    private static final int MIN_QUEUE_SIZE = 1000;
    private static final int MAX_QUEUE_SIZE = 1000000;
    private static final int MIN_TTL = 30;
    private static final int MAX_TTL = 5 * 60;
    private static final Integer MAX_NODES = 12;
    private static final Integer MIN_NODES = 1;

    TaskQueueHandler queueHandler;

    CrawlerType crawlerType;
    private final ByteStashManager<T> byteStash;

    @Builder(setterPrefix = "with")
    public ByteStashFactory(Integer nodes, Long capacity, Float hotPercent, Float warmPercent, Integer timeToLive, Integer queueSize)  {
        int nodesVal = getValidValue(nodes, MIN_NODES, MAX_NODES);
        long capacityVal = getValidValue(capacity, MIN_CAPACITY, MAX_CAPACITY);
        int ttl = getValidValue(timeToLive, MIN_TTL, MAX_TTL);
        int qSize = getValidValue(queueSize, MIN_QUEUE_SIZE, MAX_QUEUE_SIZE);
        queueHandler = new TaskQueueHandler(qSize);
        this.byteStash = new ByteStashManager<>(nodesVal, capacityVal, hotPercent, warmPercent, ttl, queueHandler, crawlerType);
    }

    private <S extends Comparable<S>> S getValidValue(S value, S min, S max) {
        return value == null ? min : (value.compareTo(min) >= 0 && value.compareTo(max) <= 0) ? value : (value.compareTo(min) < 0 ? min : max);
    }

    @Override
    public ByteStashManager<T> create() {
        return byteStash;
    }
}
