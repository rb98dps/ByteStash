package org.byteStash;

import lombok.ToString;

@ToString
public class NodeWork<T> {

    private final CacheNode<T> cacheNode;

    private final CacheRegionType cacheRegion;

    final int pos;

    public CacheNode<T> getCacheNode() {
        return cacheNode;
    }

    public CacheRegionType getCacheRegion() {
        return cacheRegion;
    }

    public int getPos() {
        return pos;
    }

    public NodeWork(CacheNode<T> node, CacheRegionType cacheRegion, int pos) {
        this.cacheRegion = cacheRegion;
        this.pos = pos;
        this.cacheNode = node;
    }
}
