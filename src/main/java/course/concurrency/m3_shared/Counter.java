package course.concurrency.m3_shared;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Counter {

    private static Object lock = new Object();

    private static int value = 1;

    public static void first() {
        for (int i = 0; i < 3; i++) {
            synchronized (lock) {
                while (value != 1) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                System.out.println(value);
                value = 2;
                lock.notifyAll();
            }
        }
    }

    public static void second() {
        for (int i = 0; i < 3; i++) {
            synchronized (lock) {
                while (value != 2) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                System.out.println(value);
                value = 3;
                lock.notifyAll();
            }
        }
    }

    public static void third() {
        for (int i = 0; i < 3; i++) {
            synchronized (lock) {
                while (value != 3) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                System.out.println(value);
                value = 1;
                lock.notifyAll();
            }
        }
    }

    public static void main(String[] args) {
        Thread t1 = new Thread(() -> first());
        Thread t2 = new Thread(() -> second());
        Thread t3 = new Thread(() -> third());
        t1.start();
        t2.start();
        t3.start();

    }
}
