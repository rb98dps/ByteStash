package org.bytestash.creator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bytestash.cache.CacheNode;
import org.bytestash.crawler.CrawlerManager;
import org.bytestash.crawler.CrawlerType;
import org.bytestash.taskhandler.TaskQueueHandler;
import org.bytestash.util.CacheUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class ByteStashManager<T> implements CacheManager<Object, T> {

    private static final Logger logger = LoggerFactory.getLogger(ByteStashManager.class);
    List<CacheNode<T>> nodes;
    CrawlerManager<T> crawlerManager;


    private final ObjectMapper objectMapper = new ObjectMapper();
    int numNodes;

    protected ByteStashManager(Integer nodesCount, Long capacity, Float hotPercent, Float warmPercent, Integer timeToLive, TaskQueueHandler queueHandler, CrawlerType type) {


        this.numNodes = nodesCount;
        long capacityPerNode = capacity / numNodes;
        hotPercent = hotPercent == null ? 0f : hotPercent;
        warmPercent = warmPercent == null ? 0f : warmPercent;
        createNodes(capacityPerNode, hotPercent, warmPercent, timeToLive);
        int noOfCrawlers = numNodes / 4 + 1;
        crawlerManager = CacheUtil.getCrawlerManagerFromType(type, nodes, noOfCrawlers,queueHandler);

    }


    private void createNodes(Long capacityPerNode, Float hotPercent, Float warmPercent, Integer timeToLive) {
        nodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            nodes.add(new CacheNode<>(capacityPerNode, hotPercent, warmPercent, timeToLive, i));
        }
        logger.debug("Successfully Created {} CacheNodes", numNodes);
    }

    public <S extends T> T get(Object keyObject, Class<S> clazz) {
        String key = getKeyFromObject(keyObject, clazz);
        if (key != null) {
            int node = getNodeFromKey(key);
            return nodes.get(node).get(key);
        }
        return null;
    }

    public T remove(Object keyObject, Class<T> clazz) {
        String key = getKeyFromObject(keyObject, clazz);
        if (key != null) {
            int node = getNodeFromKey(key);
            return nodes.get(node).remove(key);
        }
        return null;
    }

    public <S extends T> void put(Object keyObject, S value) {
        String key = getKeyFromObject(keyObject, value.getClass());
        if (key != null) {
            int node = getNodeFromKey(key);
            nodes.get(node).put(key, value);
        }
    }

    private String getKeyFromObject(Object keyObject, Class<?> clazz) {
        String key = null;
        try {
            key = generateKey(keyObject, clazz);
        } catch (IOException | NoSuchAlgorithmException e) {
            logger.error("Unable to hash the key Object {}", keyObject);
        }
        return key;
    }

    private String generateKey(Object keyObject, Class<?> clazz) throws IOException, NoSuchAlgorithmException {
        byte[] objectBytes = objectMapper.writeValueAsBytes(keyObject);
        String className = clazz.getName();
        byte[] combinedBytes = new byte[className.length() + objectBytes.length];
        System.arraycopy(className.getBytes(), 0, combinedBytes, 0, className.length());
        System.arraycopy(objectBytes, 0, combinedBytes, className.length(), objectBytes.length);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = digest.digest(combinedBytes);
        StringBuilder hexString = new StringBuilder();
        for (byte b : hashBytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    private int getNodeFromKey(String key) {
        int hashCode = key.hashCode();
        return Math.abs(hashCode) % numNodes;
    }
}
