package course.concurrency.m2_async.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

@Component
public class AsyncClassTest {

    @Autowired
    public ApplicationContext context;

    @Autowired
    @Qualifier("applicationTaskExecutor")
    private ThreadPoolTaskExecutor executor;

    @Async
    public void runAsyncTask() {
        System.out.println("getCorePoolSize: " + executor.getCorePoolSize());
        System.out.println("getMaxPoolSize: " + executor.getMaxPoolSize());
        System.out.println("getActiveCount: " + executor.getActiveCount());
        System.out.println("getKeepAliveSeconds: " + executor.getKeepAliveSeconds());

        System.out.println("runAsyncTask: " + Thread.currentThread().getName());
        executor.submit(this::internalTask);

        System.out.println("getActiveCount: " + executor.getActiveCount());
    }

    public void internalTask() {
        System.out.println("internalTask: " + Thread.currentThread().getName());
    }
}
