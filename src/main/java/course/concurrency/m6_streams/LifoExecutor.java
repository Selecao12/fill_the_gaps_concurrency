package course.concurrency.m6_streams;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LifoExecutor extends ThreadPoolExecutor {

    public LifoExecutor() {
        super(1, Integer.MAX_VALUE, 0, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>() {
            @Override
            public boolean offer(Runnable runnable) {
                return super.offerFirst(runnable);
            }

            @Override
            public boolean offer(Runnable runnable, long timeout, TimeUnit unit) throws InterruptedException {
                return super.offerFirst(runnable, timeout, unit);
            }
        });
    }

    @Override
    public void execute(Runnable command) {
        super.execute(command);
    }

    @Override
    public void close() {
        super.close();
    }
}
