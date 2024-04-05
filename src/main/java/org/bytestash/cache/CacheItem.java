package org.bytestash.cache;

import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;

class CacheItem<T> {
    @Setter
    @Getter
    private T value;
    private boolean isActive;

    @Getter
    private final int nodeNumber;

    @Setter
    @Getter
    private Timestamp timestamp;

    @Setter
    @Getter
    private CacheRegionType region;

    @Override
    public String toString() {
        return "CacheItem{" +
                       "value=" + value +
                       ", isActive=" + isActive +
                       ", timestamp=" + timestamp +
                       ", region=" + region +
                       '}';
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }


    public CacheItem(T value, CacheRegionType region, Timestamp timestamp, int nodeNumber) {
        this.value = value;
        this.isActive = false;
        this.timestamp = timestamp;
        this.region = region;
        this.nodeNumber = nodeNumber;
    }
}