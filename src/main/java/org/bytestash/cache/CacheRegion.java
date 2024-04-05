package org.bytestash.cache;

import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CacheRegion {
    LinkedHashSet<String> region;

    ConcurrentHashMap<String, Boolean> regionLocks;

    int regionMaxSize;

    ConcurrentSizeCounter counter;

    public CacheRegion(int regionSize) {
        this.regionMaxSize = regionSize;
        this.regionLocks = new ConcurrentHashMap<>();
        this.region = new LinkedHashSet<>();
        counter = new ConcurrentSizeCounter();
    }

    @Override
    public String toString() {
        return "CacheRegion{" + "region=" + region + ", regionLocks=" + regionLocks + ", counter=" + counter + '}';
    }

    public void add(String key) {
        regionLocks.put(key, false);
        region.add(key);
        counter.increment();
    }

    public void remove(String key) {
        if (contains(key)) {
            obtainLock(key);
            region.remove(key);
            regionLocks.remove(key);
            counter.decrement();
        }
    }

    private void obtainLock(String key) {
        while (regionLocks.get(key) == Boolean.TRUE) ;
        regionLocks.put(key, true);
    }

    private void releaseLock(String key) {
        regionLocks.put(key, false);
    }

    public boolean contains(String key) {
        return regionLocks.containsKey(key);
    }


    public static class ConcurrentSizeCounter {
        private final AtomicInteger size;

        public ConcurrentSizeCounter() {
            this.size = new AtomicInteger(0);
        }

        public void increment() {
            size.incrementAndGet();
        }

        public void decrement() {
            size.decrementAndGet();
        }

        public int getSize() {
            return size.get();
        }
    }

}
