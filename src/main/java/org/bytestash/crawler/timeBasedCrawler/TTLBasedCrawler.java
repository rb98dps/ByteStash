package org.bytestash.crawler.timeBasedCrawler;

import lombok.Getter;
import org.bytestash.cache.CacheRegionType;
import org.bytestash.cache.Crawlable;
import org.bytestash.crawler.Crawler;
import org.bytestash.crawler.NodeCrawler;
import org.bytestash.taskhandler.TaskQueueHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TTLBasedCrawler<T> implements Crawler<T> {
    private static final Logger logger = LoggerFactory.getLogger(TTLBasedCrawler.class);
    List<HashMap<CacheRegionType, Timestamp>> oldestTimeStamps;
    List<? extends Crawlable> crawlables;
    @Getter
    List<NodeCrawler<T>> nodeCrawlers;
    @Getter
    private final int noOfCrawlers;
    TaskQueueHandler taskQueueHandler;
    private final ScheduledExecutorService scheduler;

    protected void changeOldTimeStamp(int pos, CacheRegionType region, Timestamp timestamp) {
        logger.debug("Changed TimeStamp for Node:{} from {} to {}", pos, this.oldestTimeStamps.get(pos).get(region), timestamp);
        this.oldestTimeStamps.get(pos).put(region, timestamp);
    }


    public TTLBasedCrawler(List<? extends Crawlable> crawlables, int noOfCrawlers, TaskQueueHandler queueHandler) {
        this.crawlables = crawlables;
        oldestTimeStamps = new ArrayList<>();
        crawlables.forEach(node -> {
            HashMap<CacheRegionType, Timestamp> map = new HashMap<>();
            map.put(CacheRegionType.HOT, Timestamp.from(Instant.now()));
            map.put(CacheRegionType.WARM, Timestamp.from(Instant.now()));
            map.put(CacheRegionType.COLD, Timestamp.from(Instant.now()));
            oldestTimeStamps.add(map);
        });
        this.noOfCrawlers = noOfCrawlers;
        this.taskQueueHandler = queueHandler;
        initializeNodeCrawlers(noOfCrawlers);
        nodeCrawlers.forEach(crawler -> logger.debug(crawler.toString()));
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduleTasks();
    }

    private void scheduleTasks() {
        for (NodeCrawler<T> crawler : nodeCrawlers) {
            crawler.startTaskScheduler();
        }
        startQueueScheduler();
    }

    private void initializeNodeCrawlers(int noOfCrawlers) {
        nodeCrawlers = new ArrayList<>();
        logger.debug("Initializing Node Crawlers with count: {}", noOfCrawlers);
        for (var i = 0; i < noOfCrawlers; i++) {
            nodeCrawlers.add(new TTLBasedNodeCrawler<>(false, this, taskQueueHandler, i));
        }
    }

    public void startQueueScheduler() {
        scheduler.scheduleAtFixedRate(this::scheduleTaskToQueue, 0, 1, TimeUnit.SECONDS);
    }

    protected void scheduleTaskToQueue() {
        List<HashMap<CacheRegionType, Timestamp>> listCopy = new ArrayList<>(oldestTimeStamps);
        for (int i = 0; i < listCopy.size(); i++) {
            if (crawlables.get(i).getFilledCapacity() > 0) {
                long ttlForNode = crawlables.get(i).getTtl();
                for (Map.Entry<CacheRegionType, Timestamp> entry : listCopy.get(i).entrySet()) {
                    CacheRegionType region = entry.getKey();
                    Timestamp timestamp1 = entry.getValue();
                    Timestamp timestamp = Timestamp.from(Instant.now());
                    if (timestamp.toInstant().toEpochMilli() - timestamp1.toInstant().toEpochMilli() > (ttlForNode * 1000)) {
                        taskQueueHandler.addTask(crawlables.get(i), region, i);
                    }
                }
            }
        }
        checkStatusOfCrawlers();
    }

    private void checkStatusOfCrawlers() {
        for (NodeCrawler<T> crawler : nodeCrawlers) {
            logger.debug("Node Crawler: {}", crawler);
        }
    }

    public void stopTaskScheduler() {
        scheduler.shutdown();
    }

}
