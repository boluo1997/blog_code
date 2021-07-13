package boluo.algorithm;

import java.util.ArrayList;
import java.util.List;

public abstract class GeneticAlgorithm {

	private List<Chromosome> population = new ArrayList<>();    // 种群
	private int popSize = 100;    // 种群数量
	private int geneSize;    // 基因最大长度
	private int maxIterNum = 500;    // 最大迭代次数
	private double nutationRate = 0.01;    // 基因变异概率
	private int maxMutationNum = 3;    // 最大变异步长

	private int generation = 1;    // 当前遗传到第几代

	private double bestScore;    // 最好得分
	private double worstScore;    // 最坏得分
	private double totalScore;    // 总得分
	private double averageScore;    // 平均得分

	private double x;    // 记录历史种群中最好的x值
	private double y;    // 记录历史种群中最好的y值
	private int geneI;    // x,y所在代数

	/**
	 * @description 初始化种群, 原始的第一代
	 */
	private void init() {
		population = new ArrayList<Chromosome>();
		for (int i = 0; i < popSize; i++) {
			Chromosome chro = new Chromosome(geneSize);
			population.add(chro);
		}
		calculateScore();
	}

	/**
	 * @description 计算种群的适应度, 最好适应度, 最坏适应度, 平均适应度等
	 */
	private void calculateScore() {

	}

	/**
	 * @description 在计算个体适应度时, 需要根据基因计算对应的 y值, 这里设置两个抽象方法, 具体实现由实现类决定
	 */
	private void setChromosomeScore(Chromosome chro) {
		if (chro == null) {
			return;
		}
		double x = changeX(chro);
		double y = caculateY(x);
		chro.setScore(y);
	}

	/**
	 * @param chro
	 * @description 将二进制转化为对应的 X
	 */
	public abstract double changeX(Chromosome chro);

	/**
	 * @param x
	 * @description 根据 x 计算 y 值, Y = F(X);
	 */
	public abstract double caculateY(double x);


}


