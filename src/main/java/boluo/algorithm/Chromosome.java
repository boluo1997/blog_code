package boluo.algorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @description: 基因遗传染色体
 */
public class Chromosome {

	private boolean[] gene;    // 基因序列
	private double score;    // 对应的函数得分

	public Chromosome() {

	}

	/**
	 * @param size
	 * @description 随机生成基因序列 基因的每一个位置是0还是1, 这里采用完全随机的方式实现
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
	 * @description 将基因整体转换成对应的数字, 比如 101 -> 5
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
	 * @description 基因 num个位置发生变异 对于变异的位置, 这里采用完全随机的方式实现, 变异原则 0->1 1->0
	 */
	public void mutation(int num) {
		// 允许变异
		int size = gene.length;
		for (int i = 0; i < num; i++) {
			// 寻找变异位置
			int at = ((int) (Math.random() * size)) % size;

			// TODO 变异后的值
			boolean bool = !gene[at];
			gene[at] = bool;
		}
	}

	/**
	 * @param c
	 * @description 克隆基因, 用于产生下一代, 将已存在的基因 copy一份
	 */
	public static Chromosome clone(final Chromosome c) {
		if (c == null || c.gene == null) {
			return null;
		}
		Chromosome copy = new Chromosome();
		copy.initGeneSize(c.gene.length);
		for (int i = 0; i < c.gene.length; i++) {
			copy.gene[i] = c.gene[i];
		}
		return copy;
	}

	/**
	 * @param p1
	 * @param p2
	 * @description 父母双方产生下一代, 两个个体产生两个个体子代, 具体哪段基因产生交叉, 完全随机
	 */
	public static List<Chromosome> genetic(Chromosome p1, Chromosome p2) {

		// 染色体有一个为空, 不产生下一代
		if (p1 == null || p2 == null) {
			return null;
		}

		// 染色体有一个没有基因序列, 不产生下一代
		if (p1.gene == null || p2.gene == null) {
			return null;
		}

		// 染色体基因序列长度不一致, 不产生下一代
		if (p1.gene.length != p2.gene.length) {
			return null;
		}

		Chromosome c1 = clone(p1);
		Chromosome c2 = clone(p2);

		// 随机产生交叉互换位置
		int size = c1.gene.length;
		int a = ((int) (Math.random() * size)) % size;
		int b = ((int) (Math.random() * size)) % size;

		int min = Math.min(a, b);
		int max = Math.max(a, b);

		// 对位置上的基因进行交叉互换
		for (int i = min; i < max; i++) {
			boolean br = c1.gene[i];
			c1.gene[i] = c2.gene[i];
			c2.gene[i] = br;
		}

		List<Chromosome> list = new ArrayList<>();
		list.add(c1);
		list.add(c2);
		return list;
	}

}


