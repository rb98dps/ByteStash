package org.bytestash.cache;

public interface Cache<S, T> {
    T get(S key);
    void put(S key, T value);
    T remove(S key);
}
