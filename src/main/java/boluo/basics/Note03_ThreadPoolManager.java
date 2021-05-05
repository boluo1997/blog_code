package boluo.basics;

import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池工具类
 */
public final class Note03_ThreadPoolManager {

    private static ThreadPoolExecutor poolExecutor;

    static {

        // 核心线程数(线程池维护线程的最少数量)
        int corePoolSize = 2;

        // 线程池维护线程的最大数量
        int maximumPoolSize = 4;

        // 允许线程空闲时间
        long keepAliveTime = 60L;

        // 时间单位
        TimeUnit unit = TimeUnit.SECONDS;

        // 线程池使用的缓冲队列
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(50000);

        // 拒绝策略 由当前线程池的线程去执行， 直接调用run方法
        RejectedExecutionHandler handler = new ThreadPoolExecutor.CallerRunsPolicy();

        // 线程工厂
        ThreadFactory threadFactory = r -> {
            Thread thread = new Thread(r);
            thread.setName("BoLuo-Thread-" + Instant.now().getEpochSecond());
            return thread;
        };

        poolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit,
                workQueue, threadFactory, handler);

    }

    public static void run(Runnable task) {
        poolExecutor.execute(task);
    }

}
