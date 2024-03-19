package org.byteStash;

import javax.management.BadAttributeValueExpException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Random;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws BadAttributeValueExpException, IOException, NoSuchAlgorithmException {
        testCacheNode();
    }

    public static void testCacheNode() throws BadAttributeValueExpException {
        int count = 0;
        while (count < 10) {
            CacheNode<Object> cacheNode = new CacheNode<>(10000,0);
            Instant instant = Instant.now();
            for (int i = 0; i < 300000; i++) {

                Random random = new Random();
                int x = random.nextInt(2);

                switch (x) {
                    case 0 -> cacheNode.put(String.valueOf(i), i);
                    case 1 -> {
                        int y = random.nextInt(i + 1);
                        //System.out.println(cacheNode.get(String.valueOf(y)));
                        cacheNode.get(String.valueOf(y));
                    }
                    case 2 -> {
                        int y = random.nextInt(i + 1);
                        //System.out.println(cacheNode.remove(String.valueOf(y)));
                    }
                }
                //cacheNode.printCacheState();

            }

            System.out.println(Instant.now().toEpochMilli() - instant.toEpochMilli());

            count++;

        }
    }

}
