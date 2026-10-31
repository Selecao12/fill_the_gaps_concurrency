package course.concurrency;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.stream.IntStream;

public class Sandbox {

    public static void main(String[] args) throws InterruptedException {

        new LinkedBlockingQueue<String>();
        new ArrayBlockingQueue<String>(1);
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        forkJoinPool.submit(() -> System.out.println(
                IntStream.range(0,10000000).average().getAsDouble()
        ));
        forkJoinPool.awaitTermination(1, TimeUnit.SECONDS);
    }
}
