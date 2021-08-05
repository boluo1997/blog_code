package boluo.basics;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Note19_ThreadPool {

	// 定长线程池, 每提交一个任务就创建一个线程, 直到达到线程池的最大数量, 这时线程数量不再变化, 当线程发生错误结束时, 线程池会补充一个新的线程
	static ExecutorService fixedExecutor = Executors.newFixedThreadPool(3);

	public static void main(String[] args) {
		testFixedExecutor();
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

}
