package org.bytestash.creator;

public interface CacheManagerFactory<S,T> {
    CacheManager<S,T> create();
}
