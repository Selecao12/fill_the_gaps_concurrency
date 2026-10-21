package course.concurrency.m4_collections;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BlockingQueueTest {

    @Test
    public void shouldAddAndGetElementsWithMultipleProducersAndSingleConsumer() {

        BlockingQueue<String> blockingQueue = new BlockingQueue<>(10);

        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService producerExecutorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 100; i++) {
            producerExecutorService.submit(() -> {
                String value = UUID.randomUUID().toString();
                String threadName = Thread.currentThread().getName();
                System.out.println("Producer thread[" + threadName + "] put value: " + value);
                blockingQueue.add(value);
                try {
                    latch.await();
                } catch (InterruptedException ignored) {
                }
            });
        }

        ExecutorService consumerExecutorService = Executors.newSingleThreadExecutor();
        for (int i = 0; i < 1000; i++) {
            consumerExecutorService.submit(() -> {
                String value;
                try {
                    value = blockingQueue.get();
                } catch (Exception e) {
                    System.out.println("Got exception: " + e);
                    throw e;
                }
                String threadName = Thread.currentThread().getName();
                System.out.println("Consumer thread[" + threadName + "] got value: " + value);
                try {
                    latch.await();
                } catch (InterruptedException ignored) {
                }
            });
        }

        latch.countDown();

        producerExecutorService.shutdown();
        consumerExecutorService.shutdown();

        assertEquals(0, blockingQueue.getSize());
    }

    @Test
    public void shouldAddAndGetElementsWithMultipleProducersAndConsumers() {
        BlockingQueue<String> blockingQueue = new BlockingQueue<>(10);

        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService producerExecutorService = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 500; i++) {
            producerExecutorService.submit(() -> {
                String value = UUID.randomUUID().toString();
                String threadName = Thread.currentThread().getName();
                System.out.println("Producer thread[" + threadName + "] put value: " + value);
                blockingQueue.add(value);
                try {
                    latch.await();
                } catch (InterruptedException ignored) {
                }
            });
        }

        ExecutorService consumerExecutorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 100; i++) {
            consumerExecutorService.submit(() -> {
                String value;
                try {
                    value = blockingQueue.get();
                } catch (Exception e) {
                    System.out.println("Got exception: " + e);
                    throw e;
                }
                String threadName = Thread.currentThread().getName();
                System.out.println("Consumer thread[" + threadName + "] got value: " + value);
                try {
                    latch.await();
                } catch (InterruptedException ignored) {
                }
            });
        }

        latch.countDown();

        producerExecutorService.shutdown();
        consumerExecutorService.shutdown();

        assertEquals(0, blockingQueue.getSize());
    }
}