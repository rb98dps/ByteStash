package org.bytestash.taskhandler;

import lombok.Getter;
import lombok.ToString;
import org.bytestash.cache.CacheRegionType;
import org.bytestash.cache.Crawlable;

@ToString
@Getter
public class NodeWork {

    private final Crawlable crawlable;

    private final CacheRegionType cacheRegion;

    final int pos;

    public NodeWork(Crawlable crawlable, CacheRegionType cacheRegion, int pos) {
        this.cacheRegion = cacheRegion;
        this.pos = pos;
        this.crawlable = crawlable;
    }
}
