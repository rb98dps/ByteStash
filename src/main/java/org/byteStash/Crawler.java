package org.byteStash;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Crawler<T> {
    private static final Logger logger = LoggerFactory.getLogger(Crawler.class);
    List<HashMap<CacheRegionType, Timestamp>> oldestTimeStamps;
    List<CacheNode<T>> nodes;
    @Getter
    List<NodeCrawler<T>> crawlers;
    @Getter
    private final int noOfCrawlers;
    TaskQueueHandler<T> taskQueueHandler;
    private final ScheduledExecutorService scheduler;

    protected void changeOldTimeStamp(int pos, CacheRegionType region, Timestamp timestamp) {
        logger.debug("Changed TimeStamp for Node:{} from {} to {}", pos, this.oldestTimeStamps.get(pos).get(region), timestamp);
        this.oldestTimeStamps.get(pos).put(region, timestamp);
    }


    protected Crawler(List<CacheNode<T>> nodes, int noOfCrawlers, TaskQueueHandler<T> queueHandler) {
        this.nodes = nodes;
        oldestTimeStamps = new ArrayList<>();
        nodes.forEach(node -> {
            HashMap<CacheRegionType, Timestamp> map = new HashMap<>();
            map.put(CacheRegionType.HOT, Timestamp.from(Instant.now()));
            map.put(CacheRegionType.WARM, Timestamp.from(Instant.now()));
            map.put(CacheRegionType.COLD, Timestamp.from(Instant.now()));
            oldestTimeStamps.add(map);
        });
        this.noOfCrawlers = noOfCrawlers;
        this.taskQueueHandler = queueHandler;
        initializeNodeCrawlers(noOfCrawlers);
        crawlers.forEach(crawler -> logger.debug(crawler.toString()));
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduleTasks();
    }

    private void scheduleTasks() {
        for (NodeCrawler<T> crawler : crawlers) {

            crawler.startTaskScheduler();
        }
        //rs.forEach(NodeCrawler::startTaskScheduler);
        startQueueScheduler();
    }

    private void initializeNodeCrawlers(int noOfCrawlers) {
        crawlers = new ArrayList<>();
        logger.debug("Initializing Node Crawlers with count: {}", noOfCrawlers);
        for (var i = 0; i < noOfCrawlers; i++) {
            crawlers.add(new NodeCrawler(false, this, taskQueueHandler, i));
        }
    }

    public void startQueueScheduler() {
        // Schedule the task to run every 1 minute
        scheduler.scheduleAtFixedRate(this::scheduleTaskToQueue, 0, 1, TimeUnit.SECONDS);
    }

    protected void scheduleTaskToQueue() {
        List<HashMap<CacheRegionType, Timestamp>> listCopy = new ArrayList<>(oldestTimeStamps);
        for (int i = 0; i < listCopy.size(); i++) {
            if (nodes.get(i).getFilledCapacity() > 0) {
                long ttlForNode = nodes.get(i).getTtl();
                for (Map.Entry<CacheRegionType, Timestamp> entry : listCopy.get(i).entrySet()) {
                    CacheRegionType region = entry.getKey();
                    Timestamp timestamp1 = entry.getValue();
                    Timestamp timestamp = Timestamp.from(Instant.now());
                    if (timestamp.toInstant().toEpochMilli() - timestamp1.toInstant().toEpochMilli() > (ttlForNode * 1000)) {
                        taskQueueHandler.addTask(nodes.get(i), region, i);
                    }
                }
            }
        }
        checkStatusOfCrawlers();
    }

    private void checkStatusOfCrawlers() {
        for (NodeCrawler<T> crawler : crawlers) {
            logger.debug("Node Crawler: {}", crawler);
        }
    }

    public void stopTaskScheduler() {
        scheduler.shutdown();
    }

}
