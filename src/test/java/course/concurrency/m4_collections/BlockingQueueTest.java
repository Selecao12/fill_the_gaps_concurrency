package course.concurrency.m4_collections;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(value = 60, unit = SECONDS)
class BlockingQueueTest {

    private static final int WAIT_SEC = 5;

    // ---------------------------------------------------------------- helpers

    /** Поток с результатом. Демон, чтобы зависший тест не держал JVM. */
    private static final class Worker<R> {
        final Thread thread;
        final CompletableFuture<R> result = new CompletableFuture<>();

        Worker(Callable<R> body) {
            thread = new Thread(() -> {
                try {
                    result.complete(body.call());
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        R get() throws Exception {
            return result.get(WAIT_SEC, SECONDS);
        }

        boolean isDone() {
            return result.isDone();
        }
    }

    private static <R> Worker<R> start(Callable<R> body) {
        return new Worker<>(body);
    }

    /** Ждёт, пока поток уйдёт в park/wait. */
    private static void awaitBlocked(Worker<?> w) throws InterruptedException {
        long deadline = System.nanoTime() + SECONDS.toNanos(WAIT_SEC);
        while (true) {
            Thread.State s = w.thread.getState();
            if (s == Thread.State.WAITING || s == Thread.State.TIMED_WAITING) {
                return;
            }
            if (w.isDone() || System.nanoTime() > deadline) {
                fail("Thread did not block, state=" + s + ", done=" + w.isDone());
            }
            Thread.sleep(2);
        }
    }

    /** Поток заблокирован и после небольшой паузы всё ещё не завершился. */
    private static void assertStaysBlocked(Worker<?> w) throws InterruptedException {
        awaitBlocked(w);
        Thread.sleep(50);
        assertFalse(w.isDone(), "operation must still be blocked");
        assertTrue(w.thread.isAlive());
    }

    private static void assertInterrupted(Worker<?> w) {
        ExecutionException ex = assertThrows(ExecutionException.class, () -> w.result.get(WAIT_SEC, SECONDS));
        assertInstanceOf(InterruptedException.class, ex.getCause());
    }

    // ---------------------------------------------------------------- constructor

    @ParameterizedTest
    @ValueSource(ints = {0, -1, -100, Integer.MIN_VALUE})
    @DisplayName("capacity <= 0 is rejected")
    void nonPositiveCapacityIsRejected(int capacity) {
        assertThrows(IllegalArgumentException.class, () -> new BlockingQueue<Integer>(capacity));
    }

    @Test
    @DisplayName("huge capacity does not preallocate memory")
    void hugeCapacityIsCheap() throws Exception {
        BlockingQueue<Integer> q = new BlockingQueue<>(Integer.MAX_VALUE);
        q.enqueue(1);
        assertEquals(1, q.dequeue());
        assertEquals(Integer.MAX_VALUE, q.capacity());
    }

    // ---------------------------------------------------------------- single thread semantics

    @Test
    void newQueueIsEmpty() {
        BlockingQueue<String> q = new BlockingQueue<>(3);
        assertEquals(0, q.size());
        assertEquals(3, q.capacity());
    }

    @Test
    void elementsComeOutInFifoOrder() throws Exception {
        BlockingQueue<Integer> q = new BlockingQueue<>(5);
        for (int i = 0; i < 5; i++) {
            q.enqueue(i);
        }
        for (int i = 0; i < 5; i++) {
            assertEquals(i, q.dequeue());
        }
        assertEquals(0, q.size());
    }

    @Test
    void sizeTracksEnqueueAndDequeue() throws Exception {
        BlockingQueue<Integer> q = new BlockingQueue<>(3);
        q.enqueue(1);
        assertEquals(1, q.size());
        q.enqueue(2);
        assertEquals(2, q.size());
        q.dequeue();
        assertEquals(1, q.size());
        q.dequeue();
        assertEquals(0, q.size());
    }

    @Test
    @DisplayName("enqueue up to capacity and dequeue from non-empty queue do not block")
    void operationsWithinLimitsDoNotBlock() {
        BlockingQueue<Integer> q = new BlockingQueue<>(3);
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            q.enqueue(1);
            q.enqueue(2);
            q.enqueue(3);
            assertEquals(3, q.size());
            assertEquals(1, q.dequeue());
        });
    }

    @Test
    @DisplayName("capacity can be reused many times (ring wrap-around)")
    void capacityIsReusable() throws Exception {
        BlockingQueue<Integer> q = new BlockingQueue<>(2);
        for (int i = 0; i < 1000; i++) {
            q.enqueue(i);
            assertEquals(i, q.dequeue());
        }
        assertEquals(0, q.size());
    }

    @Test
    void capacityOneWorks() throws Exception {
        BlockingQueue<String> q = new BlockingQueue<>(1);
        q.enqueue("a");
        assertEquals(1, q.size());
        assertEquals("a", q.dequeue());
        q.enqueue("b");
        assertEquals("b", q.dequeue());
    }

    @Test
    @DisplayName("null is rejected immediately, even when the queue is full, and state is unchanged")
    void nullIsRejected() throws Exception {
        BlockingQueue<String> q = new BlockingQueue<>(1);
        assertThrows(NullPointerException.class, () -> q.enqueue(null));
        assertEquals(0, q.size());

        q.enqueue("a");
        assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                assertThrows(NullPointerException.class, () -> q.enqueue(null)));
        assertEquals(1, q.size());
        assertEquals("a", q.dequeue());
    }

    // ---------------------------------------------------------------- blocking behaviour

    @Test
    @DisplayName("dequeue blocks on empty queue until an element arrives")
    void dequeueBlocksOnEmptyQueue() throws Exception {
        BlockingQueue<String> q = new BlockingQueue<>(2);
        Worker<String> consumer = start(q::dequeue);

        assertStaysBlocked(consumer);

        q.enqueue("x");
        assertEquals("x", consumer.get());
        assertEquals(0, q.size());
    }

    @Test
    @DisplayName("enqueue blocks on full queue until space appears")
    void enqueueBlocksOnFullQueue() throws Exception {
        BlockingQueue<String> q = new BlockingQueue<>(2);
        q.enqueue("a");
        q.enqueue("b");

        Worker<Void> producer = start(() -> {
            q.enqueue("c");
            return null;
        });

        assertStaysBlocked(producer);
        assertEquals(2, q.size(), "blocked producer must not add its element");

        assertEquals("a", q.dequeue());
        producer.get();

        assertEquals(2, q.size());
        assertEquals("b", q.dequeue());
        assertEquals("c", q.dequeue());
    }

    @Test
    @DisplayName("capacity is never exceeded: extra producers wait")
    void capacityIsNeverExceeded() throws Exception {
        int capacity = 3;
        int producers = 10;
        BlockingQueue<Integer> q = new BlockingQueue<>(capacity);

        List<Worker<Void>> workers = new ArrayList<>();
        for (int i = 0; i < producers; i++) {
            int value = i;
            workers.add(start(() -> {
                q.enqueue(value);
                return null;
            }));
        }

        // ждём, пока очередь наполнится, и убеждаемся, что остальные застряли
        long deadline = System.nanoTime() + SECONDS.toNanos(WAIT_SEC);
        while (q.size() < capacity && System.nanoTime() < deadline) {
            Thread.sleep(2);
        }
        Thread.sleep(100);
        assertEquals(capacity, q.size());
        long finished = workers.stream().filter(Worker::isDone).count();
        assertEquals(capacity, finished, "exactly 'capacity' producers may complete");

        // вычитываем всё: каждый элемент ровно один раз
        Set<Integer> received = new HashSet<>();
        for (int i = 0; i < producers; i++) {
            received.add(q.dequeue());
        }
        for (Worker<Void> w : workers) {
            w.get();
        }
        assertEquals(IntStream.range(0, producers).boxed().collect(Collectors.toSet()), received);
        assertEquals(0, q.size());
    }

    @Test
    @DisplayName("all blocked consumers are woken up when elements arrive")
    void allBlockedConsumersAreWokenUp() throws Exception {
        int consumers = 5;
        BlockingQueue<Integer> q = new BlockingQueue<>(consumers);

        List<Worker<Integer>> workers = new ArrayList<>();
        for (int i = 0; i < consumers; i++) {
            workers.add(start(q::dequeue));
        }
        for (Worker<Integer> w : workers) {
            awaitBlocked(w);
        }

        for (int i = 0; i < consumers; i++) {
            q.enqueue(i);
        }

        Set<Integer> received = new HashSet<>();
        for (Worker<Integer> w : workers) {
            received.add(w.get());
        }
        assertEquals(IntStream.range(0, consumers).boxed().collect(Collectors.toSet()), received);
    }

    @Test
    @DisplayName("all blocked producers are woken up when space appears")
    void allBlockedProducersAreWokenUp() throws Exception {
        int capacity = 2;
        int producers = 5;
        BlockingQueue<Integer> q = new BlockingQueue<>(capacity);
        q.enqueue(-1);
        q.enqueue(-2);

        List<Worker<Void>> workers = new ArrayList<>();
        for (int i = 0; i < producers; i++) {
            int value = i;
            workers.add(start(() -> {
                q.enqueue(value);
                return null;
            }));
        }
        for (Worker<Void> w : workers) {
            awaitBlocked(w);
        }

        Set<Integer> received = new HashSet<>();
        for (int i = 0; i < capacity + producers; i++) {
            received.add(q.dequeue());
        }
        for (Worker<Void> w : workers) {
            w.get();
        }

        Set<Integer> expected = IntStream.range(0, producers).boxed().collect(Collectors.toSet());
        expected.add(-1);
        expected.add(-2);
        assertEquals(expected, received);
        assertEquals(0, q.size());
    }

    // ---------------------------------------------------------------- interruption

    @Test
    @DisplayName("blocked consumer reacts to interrupt: throws InterruptedException and terminates")
    void blockedConsumerCanBeInterrupted() throws Exception {
        BlockingQueue<Integer> q = new BlockingQueue<>(1);
        Worker<Integer> consumer = start(q::dequeue);
        awaitBlocked(consumer);

        consumer.thread.interrupt();

        assertInterrupted(consumer);
        consumer.thread.join(WAIT_SEC * 1000L);
        assertFalse(consumer.thread.isAlive(), "interrupted consumer must terminate, not spin");

        // очередь не сломана
        q.enqueue(7);
        assertEquals(7, q.dequeue());
    }

    @Test
    @DisplayName("blocked producer reacts to interrupt: throws InterruptedException, element is not added")
    void blockedProducerCanBeInterrupted() throws Exception {
        BlockingQueue<Integer> q = new BlockingQueue<>(1);
        q.enqueue(1);

        Worker<Void> producer = start(() -> {
            q.enqueue(2);
            return null;
        });
        awaitBlocked(producer);

        producer.thread.interrupt();

        assertInterrupted(producer);
        producer.thread.join(WAIT_SEC * 1000L);
        assertFalse(producer.thread.isAlive(), "interrupted producer must terminate, not spin");

        assertEquals(1, q.size());
        assertEquals(1, q.dequeue());
        assertEquals(0, q.size(), "value of the interrupted producer must not appear in the queue");
    }

    @Test
    @DisplayName("interrupted waiter does not steal wake-up from another waiter")
    void interruptedConsumerDoesNotLoseSignal() throws Exception {
        BlockingQueue<Integer> q = new BlockingQueue<>(1);
        Worker<Integer> c1 = start(q::dequeue);
        awaitBlocked(c1);
        Worker<Integer> c2 = start(q::dequeue);
        awaitBlocked(c2);

        c1.thread.interrupt();
        assertInterrupted(c1);

        q.enqueue(42);
        assertEquals(42, c2.get());
    }

    // ---------------------------------------------------------------- ordering

    @Test
    void singleProducerSingleConsumerKeepsOrder() throws Exception {
        int count = 20_000;
        BlockingQueue<Integer> q = new BlockingQueue<>(1);

        Worker<Void> producer = start(() -> {
            for (int i = 0; i < count; i++) {
                q.enqueue(i);
            }
            return null;
        });
        Worker<Void> consumer = start(() -> {
            for (int i = 0; i < count; i++) {
                int got = q.dequeue();
                if (got != i) {
                    throw new AssertionError("expected " + i + " but got " + got);
                }
            }
            return null;
        });

        producer.get();
        consumer.get();
        assertEquals(0, q.size());
    }

    @Test
    @DisplayName("with many producers, each producer's elements stay in order")
    void perProducerOrderIsPreserved() throws Exception {
        int producers = 4;
        int perProducer = 3_000;
        BlockingQueue<Integer> q = new BlockingQueue<>(2);

        List<Worker<Void>> workers = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int id = p;
            workers.add(start(() -> {
                for (int i = 0; i < perProducer; i++) {
                    q.enqueue(id * perProducer + i);
                }
                return null;
            }));
        }

        Worker<Void> consumer = start(() -> {
            int[] next = new int[producers];
            for (int n = 0; n < producers * perProducer; n++) {
                int item = q.dequeue();
                int id = item / perProducer;
                int seq = item % perProducer;
                if (seq != next[id]) {
                    throw new AssertionError("producer " + id + ": expected seq " + next[id] + " but got " + seq);
                }
                next[id]++;
            }
            return null;
        });

        for (Worker<Void> w : workers) {
            w.get();
        }
        consumer.get();
    }

    // ---------------------------------------------------------------- stress

    @ParameterizedTest(name = "capacity={0}, producers={1}, consumers={2}")
    @CsvSource({
            "1,   1, 1",
            "1,   4, 4",
            "3,   8, 2",
            "3,   2, 8",
            "10,  6, 6",
            "100, 4, 4"
    })
    @DisplayName("stress: every element is delivered exactly once, capacity is never exceeded")
    void everyElementIsDeliveredExactlyOnce(int capacity, int producers, int consumers) throws Exception {
        int perProducer = 5_000;
        int total = producers * perProducer;

        BlockingQueue<Integer> q = new BlockingQueue<>(capacity);
        Queue<Integer> received = new ConcurrentLinkedQueue<>();
        AtomicInteger remaining = new AtomicInteger(total);
        AtomicInteger maxObservedSize = new AtomicInteger();
        AtomicBoolean finished = new AtomicBoolean();

        ExecutorService pool = Executors.newFixedThreadPool(producers + consumers + 1);
        try {
            Future<?> sampler = pool.submit(() -> {
                while (!finished.get()) {
                    maxObservedSize.accumulateAndGet(q.size(), Math::max);
                    LockSupport.parkNanos(20_000);
                }
            });

            List<Future<?>> futures = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                int id = p;
                futures.add(pool.submit((Callable<Void>) () -> {
                    for (int i = 0; i < perProducer; i++) {
                        q.enqueue(id * perProducer + i);
                    }
                    return null;
                }));
            }
            for (int c = 0; c < consumers; c++) {
                futures.add(pool.submit((Callable<Void>) () -> {
                    while (remaining.getAndDecrement() > 0) {
                        received.add(q.dequeue());
                    }
                    return null;
                }));
            }

            for (Future<?> f : futures) {
                f.get(WAIT_SEC * 6L, SECONDS);
            }
            finished.set(true);
            sampler.get(WAIT_SEC, SECONDS);
        } finally {
            pool.shutdownNow();
        }

        assertEquals(total, received.size());
        assertEquals(IntStream.range(0, total).boxed().collect(Collectors.toSet()), new HashSet<>(received),
                "no element may be lost or duplicated");
        assertTrue(maxObservedSize.get() <= capacity,
                "observed size " + maxObservedSize.get() + " exceeds capacity " + capacity);
        assertEquals(0, q.size());
    }
}