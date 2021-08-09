package boluo.basics;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Note19_ThreadPool {

	// 定长线程池, 每提交一个任务就创建一个线程, 直到达到线程池的最大数量, 这时线程数量不再变化, 当线程发生错误结束时, 线程池会补充一个新的线程
	static ExecutorService fixedExecutor = Executors.newFixedThreadPool(3);

	// 可缓存的线程池, 如果线程池的容量超过了任务数, 自动回收空闲线程, 任务增加时可以自动添加新线程, 线程池的容量不限制
	static ExecutorService cachedExecutor = Executors.newCachedThreadPool();

	// 定时线程池(周期性线程池), 可以执行周期性的任务
	static ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(2);

	public static void main(String[] args) {
		// testFixedExecutor();
		// testCacheExecutor();
		testScheduledExecutor();
	}

	// 测试定长线程池, 线程池容量为3, 提交6个任务, 可以看出是先执行前3个任务, 前3个任务结束后再执行后面的任务
	private static void testFixedExecutor() {
		for (int i = 0; i < 6; i++) {
			final int index = i;
			fixedExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println(Thread.currentThread().getName() + " index: " + index);
				}
			});
		}

		try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("4秒后...");

		fixedExecutor.shutdown();
	}

	// 测试可缓存线程池
	private static void testCacheExecutor() {
		for (int i = 0; i < 6; i++) {
			final int index = i;
			cachedExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println(Thread.currentThread().getName() + " index: " + index);
				}
			});
		}

		try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("4秒后...");

		cachedExecutor.shutdown();
	}

	// 测试定长, 可周期执行的线程池
	private static void testScheduledExecutor() {
		for (int i = 0; i < 3; i++) {
			final int index = i;

			// scheduleWithFixedDelay 固定的延迟时间执行任务
			// scheduleAtFixedRate 固定的频率执行任务
			scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
				@Override
				public void run() {
					System.out.println(Thread.currentThread().getName() + " index: " + index);
				}
			}, 0, 2, TimeUnit.SECONDS);
		}

		try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("4秒后...");

		scheduledExecutor.shutdown();
	}


}


