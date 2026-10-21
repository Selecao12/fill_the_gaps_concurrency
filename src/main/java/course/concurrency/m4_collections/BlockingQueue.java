package course.concurrency.m4_collections;

import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;

public class BlockingQueue<T> {

    private static final Logger log = LoggerFactory.getLogger(BlockingQueue.class);

    private final Queue<T> innerQueue;
    private final int size;

    private final Object lock = new Object();

    public BlockingQueue(int size) {
        this.size = size;
        innerQueue = new ArrayDeque<>(size);
    }

    public int getSize() {
        synchronized (lock) {
            return innerQueue.size();
        }
    }

    public T get() {
        T value;
        synchronized (lock) {
            while (innerQueue.isEmpty()) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    System.out.println("Consumer thread was interrupted");
                    Thread.currentThread().interrupt();
                }
            }
            value = innerQueue.remove();
            System.out.println("get - queue size: " + innerQueue.size());
            lock.notifyAll();
        }
        return value;
    }

    public void add(T value) {
        synchronized (lock) {
            while (innerQueue.size() == size) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    System.out.println("Producer thread was interrupted");
                    Thread.currentThread().interrupt();
                }
            }
            innerQueue.add(value);
            System.out.println("add - queue size: " + innerQueue.size());
            lock.notifyAll();
        }
    }


}
