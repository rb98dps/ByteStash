package org.byteStash;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.BadAttributeValueExpException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class ByteStash<T> {

    private static final Logger logger = LoggerFactory.getLogger(ByteStash.class);
    private static final long MINIMUM_CAPACITY = 10L;
    private static final int MIN_QUEUE_SIZE = 1000;

    private static final long MINIMUM_TTL = 30L;
    List<CacheNode<T>> nodes;

    Crawler<T> crawler;

    TaskQueueHandler<T> queueHandler;

    int numNodes;

    protected ByteStash(Integer nodesCount, Long capacity, Float hotPercent, Float warmPercent, Long timeToLive, Integer queueSize) throws BadAttributeValueExpException {

        if (nodesCount == null) {
            throw new BadAttributeValueExpException("Number of Nodes can not be null");
        }
        if (capacity == null) {
            throw new BadAttributeValueExpException("Capacity can not be null");
        }
        if (nodesCount == 0) {
            throw new BadAttributeValueExpException("Nodes can not be 0");
        }
        if (capacity == 0) {
            throw new BadAttributeValueExpException("Capacity can not be 0");
        }

        this.numNodes = nodesCount;
        long capacityPerNode = checkMinimumRequirement(capacity, MINIMUM_CAPACITY)/numNodes;
        long ttl = timeToLive == null ? MINIMUM_TTL : checkMinimumRequirement(timeToLive, MINIMUM_TTL);
        int queueLength = queueSize == null ? MIN_QUEUE_SIZE : Math.max(queueSize, MIN_QUEUE_SIZE);
        hotPercent = hotPercent == null ? 0f : hotPercent;
        warmPercent = warmPercent == null ? 0f : warmPercent;

        int noOfCrawlers = numNodes / 4 + 1;
        createNodes(capacityPerNode, hotPercent, warmPercent, ttl);
        queueHandler = new TaskQueueHandler<>(queueLength);
        crawler = new Crawler<>(nodes, noOfCrawlers, queueHandler);
    }


    private long checkMinimumRequirement(long value, long minimumRequirement) {
        return Math.max(value, minimumRequirement);
    }


    private void createNodes(Long capacityPerNode, Float hotPercent, Float warmPercent, Long timeToLive) throws BadAttributeValueExpException {
        nodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            nodes.add(new CacheNode<>(capacityPerNode, hotPercent, warmPercent, timeToLive, i));
        }
        logger.debug("Successfully Created {} CacheNodes", numNodes);
    }

    public <S> T get(Object keyObject, Class<S> clazz) throws IOException, NoSuchAlgorithmException {
        String key = getKeyFromObject(keyObject, clazz);
        int node = getNodeFromKey(key);
        return nodes.get(node).get(key);
    }

    public T remove(Object keyObject, Class<T> clazz) throws IOException, NoSuchAlgorithmException {
        String key = getKeyFromObject(keyObject, clazz);

        int node = getNodeFromKey(key);
        return nodes.get(node).remove(key);
    }

    public void put(Object keyObject, T value) throws IOException, NoSuchAlgorithmException {
        String key = getKeyFromObject(keyObject, value.getClass());
        int node = getNodeFromKey(key);
        nodes.get(node).put(key, value);
    }

    private String getKeyFromObject(Object keyObject, Class<?> clazz) throws IOException, NoSuchAlgorithmException {
        String key = null;
        try {
            key = generateKey(keyObject, clazz);
        } catch (IOException | NoSuchAlgorithmException e) {
            logger.error("Unable to hash the key Object");
            throw e;
        }
        return key;
    }

    private String generateKey(Object keyObject, Class<?> clazz) throws IOException, NoSuchAlgorithmException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(keyObject);
        byte[] objectBytes = bos.toByteArray();
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
