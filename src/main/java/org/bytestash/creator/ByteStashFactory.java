package org.bytestash.creator;

import lombok.Builder;
import lombok.Getter;
import org.bytestash.crawler.CrawlerType;

import javax.management.BadAttributeValueExpException;


public class ByteStashFactory<T> {

    Integer nodes;
    Long capacity;
    Float hotPercent;
    Float warmPercent;
    Long timeToLive;
    Integer queueSize;

    CrawlerType type;

    @Getter
    private final ByteStash<T> byteStash;

    @Builder(setterPrefix = "with")
    public ByteStashFactory(Integer nodes, Long capacity, Float hotPercent, Float warmPercent, Long timeToLive, Integer queueSize) throws BadAttributeValueExpException {
        this.nodes = nodes;
        this.capacity = capacity;
        this.hotPercent = hotPercent;
        this.warmPercent = warmPercent;
        this.timeToLive = timeToLive;
        this.queueSize = queueSize;
        this.byteStash = createByteStash();
    }

    private ByteStash<T> createByteStash() throws BadAttributeValueExpException {
        return new ByteStash<>(nodes, capacity, hotPercent, warmPercent, timeToLive, queueSize,type);
    }
}
