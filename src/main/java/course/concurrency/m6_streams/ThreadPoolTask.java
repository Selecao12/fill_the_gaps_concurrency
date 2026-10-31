package course.concurrency.m6_streams;

import java.util.concurrent.*;

public class ThreadPoolTask {

    // Task #1
    public ThreadPoolExecutor getLifoExecutor() {
        return new LifoExecutor();
    }

    // Task #2
    public ThreadPoolExecutor getRejectExecutor() {
        return new RejectExecutor();
    }
}
