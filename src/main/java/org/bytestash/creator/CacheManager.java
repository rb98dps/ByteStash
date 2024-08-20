package org.bytestash.creator;

import java.io.IOException;

public interface CacheManager<Q,T>{
    <S extends T> T get(Q keyObject, Class<S> clazz);
    T remove(Q keyObject, Class<T> clazz) throws IOException;
    <S extends T> void put(Q keyObject, S value);
}
