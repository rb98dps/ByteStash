package org.bytestash.util;

import org.bytestash.cache.Crawlable;
import org.bytestash.crawler.CrawlerManager;
import org.bytestash.crawler.CrawlerType;
import org.bytestash.crawler.timeBasedCrawler.TTLBasedCrawlerManager;
import org.bytestash.taskhandler.TaskQueueHandler;

import java.util.List;

public class CacheUtil {

    private CacheUtil() {

    }

    public static <T> CrawlerManager<T> getCrawlerManagerFromType(CrawlerType crawlerType,
                                                                  List<? extends Crawlable> crawlables,int count,
                                                                  TaskQueueHandler queueHandler) {
        CrawlerManager<T> crawlerManager;
        switch (crawlerType) {
            case TTL -> crawlerManager = new TTLBasedCrawlerManager<>(crawlables, count, queueHandler);
            default -> crawlerManager = new TTLBasedCrawlerManager<>(crawlables, count, queueHandler);
        }
        return crawlerManager;
    }
}
