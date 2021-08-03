package boluo.basics;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.primitives.Ints;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

import java.util.*;

public class Note18_Guava {

	// 连接器
	private static final Joiner joiner = Joiner.on(",").skipNulls();

	// 分割器
	private static final Splitter splitter = Splitter.on(",").trimResults().omitEmptyStrings();

	// 字符串匹配器
	private static final CharMatcher charMatcherDigit = CharMatcher.digit();
	private static final CharMatcher charMatcherAny = CharMatcher.any();

	@Test
	public void func1() {
		// 把集合, 数组中的元素join在一起
		String join = joiner.join(Lists.newArrayList("a", null, "b"));
		System.out.println("join = " + join);

		for (String tmp : splitter.split("a,  ,b,,")) {
			System.out.println("|" + tmp + "|");
		}
	}

	@Test
	public void func2() {
		// 只保留匹配的字符, 其他移除
		System.out.println(charMatcherDigit.retainFrom("qwertyuiopasdfghjkl"));

		// 移除匹配的字符
		System.out.println(charMatcherDigit.removeFrom("yeah I love u 1314"));

	}

	@Test
	public void func3() {
		// guava对jdk原生类型操作的扩展
		List<Integer> list = Ints.asList(1, 3, 5, 7, 9);

		System.out.println(Ints.join(",", 1, 3, 1, 4));

		// 原生类型数组快速合并
		int[] newIntArray = Ints.concat(new int[]{1, 2}, new int[]{2, 3, 4});
		System.out.println(newIntArray.length);

		// 最大/最小
		System.out.println(Ints.max(newIntArray) + "," + Ints.min(newIntArray));

		// 是否包含
		System.out.println(Ints.contains(newIntArray, 6));

		// 集合到数组的转换
		int[] toArray = Ints.toArray(list);

	}

	@Test
	public void func4() {

		// 对JDK集合的有效补充 Multiset(无序的, 但是可以重复的集合)
		Multiset<String> multiset = HashMultiset.create();
		multiset.add("a");
		multiset.add("a");
		multiset.add("b");
		multiset.add("c");
		multiset.add("b");

		System.out.println(multiset.size());
		// Multiset自带功能, 可以跟踪每个对象的数量
		System.out.println(multiset.count("a"));

	}

	@Test
	public void func5() {

		// List的不可变性设置
		List<String> list = new ArrayList<>();
		list.add("a");
		list.add("b");

		// 这种视图不够安全, 不是真正意义上的快照
		List<String> readOnlyList = Collections.unmodifiableList(list);

		// readOnlyList.add("c"); java.long.UnsupportedOperationException
		list.add("c");

		System.out.println(readOnlyList);

		// 实际上, Collections.unmodifiableXxx所返回的集合和源集合是同一个对象, 只不过可以对集合做出改变的API都被重写,
		// 会抛出UnsupportedOperationException
		// 也就是说我们改变源集合, 会导致不可变视图(unmodifiable View)也会发生变化
	}

	@Test
	public void func6() {

		// 在不使用guava的情况下, 避免上面的问题
		List<String> list = new ArrayList<>();
		list.add("a");
		list.add("b");

		// Defensive Copy, 保护性拷贝
		List<String> readOblyList = Collections.unmodifiableList(new ArrayList<String>(list));

	}

	@Test
	public void func7() {
		// 为了改进unmodifiable, guava提出了Immutable的概念
		List<String> immutable = ImmutableList.of("a", "b", "c");
		// immutable.add("d");		// java.lang.UnsupportedOperationException

		List<String> list = new ArrayList<>();
		list.add("a");
		List<String> immutable2 = ImmutableList.copyOf(list);
		list.add("d");

		// 视图不随着源而改变
		System.out.println("list size : " + list.size() + ", immutable.size : " + immutable2.size());
	}

	@Test
	public void func8() {
		// 一对多数据结构
		// JDK中的map是一对一结构的, 如果需要一对多结构的, 往往表达成: Map<key, List>, 比较臃肿
		// guava中使用 Multimap

		// guava中所有的集合都具有create方法
		Multimap<String, String> multimap = ArrayListMultimap.create();
		multimap.put("boluo", "1");
		multimap.put("boluo", "2");
		multimap.put("dingc", "1");

		System.out.println(multimap.get("boluo"));    // collection
	}

	@Test
	public void func9() {
		// 双向Map : BiMap
		BiMap<String, String> biMap = HashBiMap.create();
		biMap.put("name", "dingc");

		// value重复会报错,
		// biMap.put("nick", "dingc");

		// 强制覆盖
		biMap.forcePut("nick", "dingc");
		biMap.put("gender", "man");
		System.out.println(biMap.inverse().get("man"));

		// BiMap内部维护了两个map
		Preconditions.checkArgument(biMap.inverse().inverse() == biMap, "");
	}

	@Test
	public void func10() {
		// 多个key: Table, 类似于Map<k1, Map<k2, v2>>
		Table<String, String, Integer> table = HashBasedTable.create();
		table.put("dingc", "语文", 80);
		table.put("dingc", "数学", 90);
		table.put("qidai", "语文", 80);
		table.put("qidai", "数学", 90);
		table.put("boluo", "英语", 80);

		// 最小单位: cell
		Set<Table.Cell<String, String, Integer>> set = table.cellSet();
		for (Table.Cell cell : set) {
			System.out.println(cell.getRowKey() + ", " + cell.getColumnKey() + ", " + cell.getValue());
		}

		// row set
		Set<String> rowSet = table.rowKeySet();
		System.out.println(rowSet);

		// column set
		Set<String> columnSet = table.columnKeySet();
		System.out.println(columnSet);

		// 根据rowKey获得信息Map<column, value>	{语文=80, 数学=90}
		System.out.println(table.row("dingc"));

		// 根据column获得信息Map<row, key>
		System.out.println(table.column("语文"));
	}

	@Test
	public void func11() {
		// 函数式编程 Functions
		List<String> list = Lists.newArrayList("dingc", "hello", "world");

		Function<String, String> f1 = new Function<String, String>() {
			@Override
			public @Nullable String apply(@Nullable String s) {
				return s.length() < 6 ? s : s.substring(0, 5);
			}
		};

		Function<String, String> f2 = new Function<String, String>() {
			@Override
			public @Nullable String apply(@Nullable String s) {
				return s.toUpperCase();
			}
		};

		Function<String, String> f3 = Functions.compose(f1, f2);

		Collection<String> collection = Collections2.transform(list, f3);
		collection.forEach(System.out::println);
	}

	@Test
	public void func12() {
		// Predicate

	}

}









