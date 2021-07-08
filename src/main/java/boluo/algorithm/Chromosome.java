package boluo.algorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @Description: 基因遗传染色体
 */
public class Chromosome {

	private boolean[] gene;    // 基因序列
	private double score;    // 对应的函数得分

	/**
	 * @param size
	 * @Description 随机生成基因序列 基因的每一个位置是0还是1, 这里采用完全随机的方式实现
	 */
	public Chromosome(int size) {
		if (size <= 0) {
			return;
		}
		initGeneSize(size);
		for (int i = 0; i < size; i++) {
			gene[i] = Math.random() >= 0.5;
		}
	}

	public void initGeneSize(int size) {
		if (size <= 0) {
			return;
		}
		gene = new boolean[size];
	}

	/**
	 * @return
	 * @Description 将基因整体转换成对应的数字, 比如 101 -> 5
	 */
	public int getNum() {
		if (gene == null) {
			return 0;
		}
		int num = 0;
		for (boolean bool : gene) {
			num <<= 1;
			if (bool) {
				num += 1;
			}
		}
		return num;
	}

	/**
	 * @param num
	 * @Description 基因 num个位置发生变异 对于变异的位置, 这里采用完全随机的方式实现, 变异原则 0->1 1->0
	 */
	public void mutation(int num) {
		// 允许变异
		int size = gene.length;
		for (int i = 0; i < num; i++) {
			// 寻找变异位置
			int at = ((int) (Math.random() * size)) % size;

			// TODO 变异后的值
		}
	}

}


