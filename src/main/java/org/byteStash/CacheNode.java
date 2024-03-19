package org.byteStash;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.BadAttributeValueExpException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CacheNode<T> {

    private static final Logger logger = LoggerFactory.getLogger(CacheNode.class);
    private long hotRegionSize;
    private long warmRegionSize;
    private long coldRegionSize;

    @Getter
    private final long capacity;

    @Getter
    private long filledCapacity = 0;

    @Getter
    private long ttl;

    private ConcurrentHashMap<String, CacheItem<T>> localCache;

    protected Map<CacheRegion, LinkedHashSet<String>> regions;

    private final int index;

    public CacheNode(long capacity, int index) throws BadAttributeValueExpException {
        this(capacity, 0f, 0f, 240, index);
    }

    public CacheNode(long capacity, long ttl, int index) throws BadAttributeValueExpException {
        this(capacity, 0f, 0f, ttl, index);
    }

    public CacheNode(long capacity, float hotPercent, float warmPercent, long ttl, int index) throws BadAttributeValueExpException {
        this.index = index;
        if (hotPercent != 0f && warmPercent != 0f) {
            if (hotPercent + warmPercent > 0.5) {
                throw new BadAttributeValueExpException("Hot and Cold can not be greater than 0.5");
            }
            generateCache(capacity, hotPercent, warmPercent, 1 - hotPercent - warmPercent);
        } else {
            generateCache(capacity, 0.1f, 0.2f, 0.7f);
        }
        this.ttl = ttl;
        this.capacity = capacity;

    }


    Timestamp removeAllExpiredItems(CacheRegion region) {
        Timestamp oldestTimestamp = Timestamp.from(Instant.now());
        List<String> set = new ArrayList<>(regions.get(region));
        for (String key : set) {
            Timestamp timestamp = removeKeyIfExpired(key, region);
            if (timestamp != null) {
                int val = timestamp.compareTo(oldestTimestamp);
                if (val < 0) {
                    oldestTimestamp = timestamp;
                }
            }
        }
        return oldestTimestamp;
    }

    public Timestamp removeKeyIfExpired(String key, CacheRegion region) {
        CacheItem<T> item = localCache.get(key);

        long life = (Instant.now().toEpochMilli() - item.getTimestamp().toInstant().toEpochMilli()) / 1000;
        if (life > ttl) {
            remove(key);
        } else {
            return item.getTimestamp();
        }
        return null;
    }

    private void generateCache(long capacity, float hotPercent, float warmPercent, float coldPercent) {

        hotRegionSize = (long) (capacity * hotPercent);
        warmRegionSize = (long) (capacity * warmPercent);
        coldRegionSize = (long) (capacity * coldPercent);
        regions = new HashMap<>();
        regions.put(CacheRegion.HOT, new LinkedHashSet<>((int) hotRegionSize));
        regions.put(CacheRegion.WARM, new LinkedHashSet<>((int) warmRegionSize));
        regions.put(CacheRegion.COLD, new LinkedHashSet<>((int) coldRegionSize));
        localCache = new ConcurrentHashMap<>();
    }

    public void put(String key, T value) {
        if (localCache.containsKey(key)) {
            CacheItem<T> item = localCache.get(key);
            item.setValue(value);
            item.setActive(true);
            if (!CacheRegion.HOT.equals(item.getRegion())) {
                addKeyToRegion(key, CacheRegion.WARM, item);
            }
            item.setTimestamp(Timestamp.from(Instant.now()));
        } else {
            localCache.put(key, new CacheItem<>(value, CacheRegion.HOT, Timestamp.from(Instant.now()), index));
            filledCapacity++;
            regions.get(CacheRegion.HOT).add(key);
            ensureHotRegionSize();
        }
    }

    public T get(String key) {
        if (localCache.containsKey(key)) {
            CacheItem<T> item = localCache.get(key);
            keyAccessedAgain(key, item);
            item.setTimestamp(Timestamp.from(Instant.now()));
            return item.getValue();
        }
        return null;
    }

    private void keyAccessedAgain(String key, CacheItem<T> item) {
        item.setActive(true);
        CacheRegion region = item.getRegion();
        if (!CacheRegion.HOT.equals(region)) {
            addKeyToRegion(key, CacheRegion.WARM, item);
        }
    }

    private void addKeyToRegion(String key, CacheRegion region, CacheItem<T> item) {
        removeFromRegion(key);
        regions.get(region).add(key);
        item.setRegion(region);
        ensureRegionSize(region);
    }

    public T remove(String key) {
        if (localCache.containsKey(key)) {
            CacheItem<T> removedItem = localCache.remove(key);
            if (null != removedItem) {
                regions.get(removedItem.getRegion()).remove(key);
                filledCapacity--;
                return removedItem.getValue();
            }
        }
        return null;
    }

    private void ensureRegionSize(CacheRegion region) {
        switch (region) {
            case HOT -> ensureHotRegionSize();
            case WARM -> ensureWarmRegionSize();
            case COLD -> ensureColdRegionSize();
        }
    }

    public void removeFromRegion(String key) {
        if (localCache.containsKey(key)) {
            CacheItem<T> item = localCache.get(key);
            regions.get(item.getRegion()).remove(key);
        }
    }

    private void ensureHotRegionSize() {

        LinkedHashSet<String> hotRegion = regions.get(CacheRegion.HOT);
        while (hotRegion.size() > hotRegionSize) {
            String key = hotRegion.iterator().next();
            if (!localCache.get(key).isActive()) {
                addKeyToRegion(key, CacheRegion.COLD, localCache.get(key));
            } else {
                addKeyToRegion(key, CacheRegion.WARM, localCache.get(key));
            }
        }
    }

    private void ensureWarmRegionSize() {
        LinkedHashSet<String> warmRegion = regions.get(CacheRegion.WARM);
        while (warmRegion.size() > warmRegionSize) {
            String key = warmRegion.iterator().next();
            addKeyToRegion(key, CacheRegion.COLD, localCache.get(key));
        }
    }

    private void ensureColdRegionSize() {

        LinkedHashSet<String> coldRegion = regions.get(CacheRegion.COLD);
        while (coldRegion.size() > coldRegionSize) {
            String key = coldRegion.iterator().next();
            remove(key);
        }
    }

    public void printCacheState() {
        logger.debug("Node Capacity {}, hot size: {}, warm size: {} , cold size: {}", capacity, hotRegionSize, warmRegionSize, coldRegionSize);
        logger.debug("Node : {} , Cache State: {} ", index, localCache);
        logger.debug("Node : {} , Hot Region: {}", index, regions.get(CacheRegion.HOT));
        logger.debug("Node : {} , Warm Region: {}", index, regions.get(CacheRegion.WARM));
        logger.debug("Node : {} , Cold Region: {}", index, regions.get(CacheRegion.COLD));
    }
}
