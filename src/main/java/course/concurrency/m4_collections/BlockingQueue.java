package course.concurrency.m4_collections;

import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;

public class BlockingQueue<T> {

    private static final Logger log = LoggerFactory.getLogger(BlockingQueue.class);

    private final Queue<T> innerQueue;
    private final int capacity;

    private final Object lock = new Object();

    public BlockingQueue(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException();
        this.capacity = capacity;
        innerQueue = new ArrayDeque<>();
    }

    public int size() {
        synchronized (lock) {
            return innerQueue.size();
        }
    }

    public int capacity() {
        return capacity;
    }

    public T dequeue() throws InterruptedException {
        T value;
        synchronized (lock) {
            while (innerQueue.isEmpty()) {
                lock.wait();
            }
            value = innerQueue.remove();
            System.out.println("get - queue size: " + innerQueue.size());
            lock.notifyAll();
        }
        return value;
    }

    public void enqueue(T value) throws InterruptedException {
        Objects.requireNonNull(value);
        synchronized (lock) {
            while (innerQueue.size() == capacity) {
                lock.wait();
            }
            innerQueue.add(value);
            System.out.println("add - queue size: " + innerQueue.size());
            lock.notifyAll();
        }
    }


}
