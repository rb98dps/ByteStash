package org.bytestash.cache;

import org.bytestash.evictionpolicy.EvictionInfo;

public interface Crawlable {
    EvictionInfo removeItems(CacheRegionType region);

    void printCacheState();

    long getTtl();

    long getFilledCapacity();
}
