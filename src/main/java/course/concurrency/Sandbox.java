package course.concurrency;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicMarkableReference;

public class Sandbox {

    public static void main(String[] args) {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

        map.put("key1", "val1");
        map.put("key2", "val2");

        Thread thread1 = new Thread(() -> {
            map.compute("key1", (t1, t2) -> {
                map.compute("key2", (v1, v2) -> "1");
                return "";
            });
        });
        thread1.setName("thread1");
        Thread thread2 = new Thread(() -> {
            map.compute("key2", (t1, t2) -> {
                map.compute("key1", (v1, v2) -> "1");
                return "";
            });

        });
        thread2.setName("thread2");

        thread1.start();
        thread2.start();

        AtomicMarkableReference<String> markableReference = new AtomicMarkableReference<>("dasd", false);

        boolean[] markHolder = new boolean[1];
        String s = markableReference.get(markHolder);

        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
