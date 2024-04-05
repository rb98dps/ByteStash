package org.bytestash.crawler;

import org.bytestash.cache.CacheRegionType;
import org.bytestash.cache.Crawlable;

public interface NodeCrawler<T> {

    void nodeCleanup(Crawlable crawlable, CacheRegionType cacheRegion, int pos);

    void getTask();

    void startTaskScheduler();
}
