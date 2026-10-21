package course.concurrency.m3_shared.testing;

import org.junit.jupiter.api.RepeatedTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestExperiments {

    // Don't change this class
    public static class Counter {
        private volatile int counter = 0;

        public void increment() {
            counter++;
        }

        public int get() {
            return counter;
        }
    }

    @RepeatedTest(1000)
    public void counterShouldFail() {
        Counter counter = new Counter();
        int iterations = 5_000;
        int threads = Runtime.getRuntime().availableProcessors();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        try (ExecutorService executorService = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < iterations; i++) {
                executorService.submit(() ->
                        {
                            try {
                                countDownLatch.await();
                            } catch (InterruptedException ignored) {
                            }
                            counter.increment();
                        }
                );
            }
            countDownLatch.countDown();
        }

        assertEquals(iterations, counter.get());
    }
}
