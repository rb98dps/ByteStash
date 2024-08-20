package org.bytestash.cache;

import lombok.Getter;
import org.bytestash.evictionpolicy.EvictionInfo;
import org.bytestash.evictionpolicy.TimeStampBasedEvictionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.BadAttributeValueExpException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CacheNode<T> implements Cache<String, T>, Crawlable {

    private static final Logger logger = LoggerFactory.getLogger(CacheNode.class);
    private long hotRegionSize;
    private long warmRegionSize;
    private long coldRegionSize;

    @Getter
    private final long capacity;

    private long filledCapacity = 0;

    @Override
    public long getFilledCapacity() {
        return filledCapacity;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    private final int ttl;

    private ConcurrentHashMap<String, CacheItem<T>> localCache;

    private ConcurrentHashMap<String, CacheItem<T>> localHotCache;

    protected Map<CacheRegionType, CacheRegion> regions;

    private final int index;

    public CacheNode(long capacity, int index) {
        this(capacity, 0f, 0f, 240, index);
    }

    public CacheNode(long capacity, int ttl, int index) {
        this(capacity, 0f, 0f, ttl, index);
    }

    public CacheNode(long capacity, float hotPercent, float warmPercent, int ttl, int index) {
        this.index = index;
        if (hotPercent != 0f && warmPercent != 0f) {
            if (hotPercent + warmPercent > 0.5) {
                generateCache(capacity, 0.1f, 0.2f, 0.7f);
            } else {
                generateCache(capacity, hotPercent, warmPercent, 1 - hotPercent - warmPercent);
            }
        } else {
            generateCache(capacity, 0.1f, 0.2f, 0.7f);
        }
        this.ttl = ttl;
        this.capacity = capacity;

    }

    private void generateCache(long capacity, float hotPercent, float warmPercent, float coldPercent) {

        hotRegionSize = (long) (capacity * hotPercent);
        warmRegionSize = (long) (capacity * warmPercent);
        coldRegionSize = (long) (capacity * coldPercent);
        regions = new HashMap<>();
        regions.put(CacheRegionType.HOT, new CacheRegion((int) hotRegionSize));
        regions.put(CacheRegionType.WARM, new CacheRegion((int) warmRegionSize));
        regions.put(CacheRegionType.COLD, new CacheRegion((int) coldRegionSize));
        localCache = new ConcurrentHashMap<>();
        localHotCache = new ConcurrentHashMap<>();
    }

    public void put(String key, T value) {

        if (localHotCache.containsKey(key)) {
            CacheItem<T> item = localHotCache.get(key);
            item.setValue(value);
            item.setActive(true);
            item.setTimestamp(Timestamp.from(Instant.now()));
        } else if (localCache.containsKey(key)) {
            CacheItem<T> item = localCache.get(key);
            item.setValue(value);
            item.setActive(true);
            if (!CacheRegionType.WARM.equals(item.getRegion())) {
                addKeyToDifferentRegion(key, CacheRegionType.WARM, item);
            }
            item.setTimestamp(Timestamp.from(Instant.now()));
        } else {
            localHotCache.put(key, new CacheItem<>(value, CacheRegionType.HOT, Timestamp.from(Instant.now()), index));
            filledCapacity++;
            addNewKeyToRegion(key, CacheRegionType.HOT);
            ensureHotRegionSize();
        }
    }

    public void addNewKeyToRegion(String key, CacheRegionType region) {
        regions.get(region).add(key);
    }

    public T get(String key) {
        if (localHotCache.containsKey(key)) {
            return updateKeyDetails(key, localHotCache);
        }
        if (localCache.containsKey(key)) {
            return updateKeyDetails(key, localCache);
        }
        return null;
    }

    private T updateKeyDetails(String key, ConcurrentHashMap<String, CacheItem<T>> cache) {
        CacheItem<T> item = cache.get(key);
        keyAccessedAgain(key, item);
        item.setTimestamp(Timestamp.from(Instant.now()));
        return item.getValue();
    }

    private void keyAccessedAgain(String key, CacheItem<T> item) {
        item.setActive(true);
        CacheRegionType region = item.getRegion();
        if (!CacheRegionType.HOT.equals(region)) {
            addKeyToDifferentRegion(key, CacheRegionType.WARM, item);
        }
    }

    private void addKeyToDifferentRegion(String key, CacheRegionType region, CacheItem<T> item) {
        removeFromRegion(key);
        regions.get(region).add(key);
        item.setRegion(region);
        ensureRegionSize(region);
    }

    public T remove(String key) {
        CacheItem<T> item = removeFromCacheAndRegion(key, localHotCache);
        item = item != null ? item : removeFromCacheAndRegion(key, localCache);
        return item != null ? item.getValue() : null;
    }

    public void transferFromHotCache(String key) {
        CacheItem<T> item = removeFromCacheAndRegion(key, localHotCache);
        if (!item.isActive()) {
            localCache.put(key, item);
            filledCapacity++;
            addKeyToDifferentRegion(key, CacheRegionType.COLD, item);
        } else {
            localCache.put(key, item);
            filledCapacity++;
            addKeyToDifferentRegion(key, CacheRegionType.WARM, item);
        }
    }

    private CacheItem<T> removeFromCacheAndRegion(String key, ConcurrentMap<String, CacheItem<T>> cache) {
        if (cache.containsKey(key)) {
            CacheItem<T> removedItem = cache.remove(key);
            if (null != removedItem) {
                regions.get(removedItem.getRegion()).remove(key);
                filledCapacity--;
                return removedItem;
            }
        }
        return null;
    }


    public void removeFromRegion(String key) {
        if (localHotCache.containsKey(key)) {
            CacheItem<T> item = localHotCache.get(key);
            regions.get(item.getRegion()).remove(key);
        } else if (localCache.containsKey(key)) {
            CacheItem<T> item = localCache.get(key);
            regions.get(item.getRegion()).remove(key);
        }
    }

    public EvictionInfo removeItems(CacheRegionType region) {
        Timestamp oldestTimestamp = Timestamp.from(Instant.now());
        List<String> set = new ArrayList<>(regions.get(region).region);
        for (String key : set) {
            Timestamp timestamp = removeKeyIfExpired(key, region);
            if (timestamp != null) {
                int val = timestamp.compareTo(oldestTimestamp);
                if (val < 0) {
                    oldestTimestamp = timestamp;
                }
            }
        }

        return new TimeStampBasedEvictionInfo(oldestTimestamp);
    }

    public Timestamp removeKeyIfExpired(String key, CacheRegionType region) {
        if (region.equals(CacheRegionType.HOT)) {
            CacheItem<T> item = localHotCache.get(key);
            long life = (Instant.now().toEpochMilli() - item.getTimestamp().toInstant().toEpochMilli()) / 1000;
            if (life > ttl) {
                remove(key);
            } else {
                return item.getTimestamp();
            }
        } else {
            CacheItem<T> item = localCache.get(key);
            long life = (Instant.now().toEpochMilli() - item.getTimestamp().toInstant().toEpochMilli()) / 1000;
            if (life > ttl) {
                remove(key);
            } else {
                return item.getTimestamp();
            }
        }
        return null;
    }

    private void ensureRegionSize(CacheRegionType region) {
        switch (region) {
            case HOT -> ensureHotRegionSize();
            case WARM -> ensureWarmRegionSize();
            case COLD -> ensureColdRegionSize();
        }
    }

    private void ensureHotRegionSize() {

        CacheRegion cacheRegion = regions.get(CacheRegionType.HOT);
        while (cacheRegion.counter.getSize() > hotRegionSize) {
            String key = cacheRegion.region.iterator().next();
            transferFromHotCache(key);
        }
    }

    private void ensureWarmRegionSize() {
        CacheRegion cacheRegion = regions.get(CacheRegionType.WARM);
        while (cacheRegion.counter.getSize() > warmRegionSize) {
            String key = cacheRegion.region.iterator().next();
            addKeyToDifferentRegion(key, CacheRegionType.COLD, localCache.get(key));
        }
    }

    private void ensureColdRegionSize() {

        CacheRegion cacheRegion = regions.get(CacheRegionType.COLD);
        while (cacheRegion.counter.getSize() > coldRegionSize) {
            String key = cacheRegion.region.iterator().next();
            remove(key);
        }
    }

    public void printCacheState() {
        logger.debug("Node filledCapacity {}, hot size: {}, warm size: {} , cold size: {}", filledCapacity, regions.get(CacheRegionType.HOT).counter.getSize(), regions.get(CacheRegionType.WARM).counter.getSize(), regions.get(CacheRegionType.COLD).counter.getSize());
        logger.debug("Node : {} , Hot Cache State: {} ", index, localHotCache.keySet());
        logger.debug("Node : {} , Cache State: {} ", index, localCache.keySet());
        logger.debug("Node : {} , Hot Region: {}", index, regions.get(CacheRegionType.HOT));
        logger.debug("Node : {} , Warm Region: {}", index, regions.get(CacheRegionType.WARM));
        logger.debug("Node : {} , Cold Region: {}", index, regions.get(CacheRegionType.COLD));
    }

    public void checkCacheAndRegion() {

        var test1 = localHotCache.keySet().equals(regions.get(CacheRegionType.HOT).region);
        HashSet<String> set = new HashSet<>(regions.get(CacheRegionType.WARM).region);
        set.addAll(regions.get(CacheRegionType.COLD).region);
        var test2 = localCache.keySet().equals(set);
        var test3 = filledCapacity == (localCache.size() + localHotCache.size());

        if (!test1 || !test2 || !test3) {
            throw new RuntimeException("Found Bug");
        }

    }

}
