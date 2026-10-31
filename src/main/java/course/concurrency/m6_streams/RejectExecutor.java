package course.concurrency.m6_streams;

import java.util.concurrent.*;

public class RejectExecutor extends ThreadPoolExecutor {

    public RejectExecutor() {
        super(8, 8, 0, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new ThreadPoolExecutor.DiscardPolicy());
    }
}
