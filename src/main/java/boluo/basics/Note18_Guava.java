package boluo.basics;

import com.google.common.base.*;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import com.google.common.primitives.Ints;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import boluo.model.User;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Note18_Guava {

	// 连接器
	private static final Joiner joiner = Joiner.on(",").skipNulls();

	// 分割器
	private static final Splitter splitter = Splitter.on(",").trimResults().omitEmptyStrings();

	// 字符串匹配器
	private static final CharMatcher charMatcherDigit = CharMatcher.digit();
	private static final CharMatcher charMatcherAny = CharMatcher.any();

	// 定义缓存的实现
	private static final CacheLoader<Long, User> userCacheLoader = new CacheLoader<Long, User>() {
		@Override
		public User load(Long aLong) throws Exception {

			// 模拟从数据库/Redis/缓存中加载数据
			User user = new User();
			user.setUserId(aLong);
			user.setName(Thread.currentThread().getName() + "-" +
					new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + "-" + aLong);

			System.out.println("load: " + user);
			return user;
		}
	};

	// 定义缓存的策略, 提供对外访问缓存
	private static final LoadingCache<Long, User> userCacheDate = CacheBuilder.newBuilder()
			.expireAfterAccess(2, TimeUnit.SECONDS)
			.expireAfterWrite(2, TimeUnit.SECONDS)
			.refreshAfterWrite(3, TimeUnit.SECONDS)
			.maximumSize(10000L)
			.build(userCacheLoader);

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
		// Predicate, 最常用的功能就是运用在集合的过滤当中
		List<String> list = Lists.newArrayList("dingc", "hello", "world");

		Collection<String> collection = Collections2.filter(list, new Predicate<String>() {
			@Override
			public boolean apply(@Nullable String s) {
				// 业务逻辑
				return new StringBuilder(s).reverse().toString().equals(s);
			}
		});

		collection.forEach(System.out::println);
	}

	@Test
	public void func13() {
		// check null and other : Optional 、Preconditions

		String name = "name";
		int age = 20;
		Preconditions.checkNotNull(name, "name must be given");
		Preconditions.checkArgument(age > 18, "the game you can't play it, you age is under 18!");

		Map<String, String> defaultExtInfo = Maps.newHashMap();
		defaultExtInfo.put("sex", "man");

		Map<String, String> extInfo = Maps.newHashMap();
		extInfo = Optional.fromNullable(extInfo).or(defaultExtInfo);

		for (Map.Entry<String, String> entry : extInfo.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
	}

	@Test
	public void func14() throws ExecutionException {
		// Cache is King
		// 不想使用第三方缓存组件(如redis)的时候, 可以使用guava提供的本地缓存
		User user = new User();
		user.setUserId(0);
		user.setName("dingc");
		userCacheDate.put(0L, user);
		System.out.println(userCacheDate.get(0L));
	}

	@Test
	public void func15() throws ExecutionException {

		// guava缓存测试
		CacheLoader<String, String> loader = new CacheLoader<String, String>() {
			@Override
			public String load(String key) throws Exception {
				Thread.sleep(1000);    // 休眠1s, 模拟加载数据
				System.out.println(key + " is loaded from a cacheLoader!");
				return key + "'s value";
			}
		};

		LoadingCache<String, String> loadingCache = CacheBuilder.newBuilder()
				.maximumSize(3)        // 最大存储, 超过将把之前的缓存删掉
				.build(loader);        // 在构建时指定自动加载器

		loadingCache.get("key1");
		loadingCache.get("key2");

		LoadingCache<String, String> apiValidRulecache = CacheBuilder.newBuilder()
				.initialCapacity(10)
				.expireAfterWrite(10, TimeUnit.MINUTES)
				.build(new CacheLoader<String, String>() {
					@Override
					public String load(String key) throws Exception {
						Thread.sleep(1000); //休眠1s，模拟加载数据
						System.out.println(key + " is loaded from a cacheLoader!");
						return key + "'s value";
					}
				});

		apiValidRulecache.get("key3");
		apiValidRulecache.get("key4");
		apiValidRulecache.get("key3");
		System.out.println(apiValidRulecache.get("key1"));
		System.out.println(apiValidRulecache.get("key2"));
		System.out.println(apiValidRulecache.get("key3"));
		System.out.println(apiValidRulecache.get("key4"));

		Cache<String,String> cache = CacheBuilder.newBuilder()
				.maximumSize(3)
				.recordStats() //开启统计信息开关
				.build();
		cache.put("key1","value1");
		cache.put("key2","value2");
		cache.put("key3","value3");
		cache.put("key4","value4");

		cache.getIfPresent("key1");
		cache.getIfPresent("key2");
		cache.getIfPresent("key3");
		cache.getIfPresent("key4");
		cache.getIfPresent("key5");
		cache.getIfPresent("key6");

		System.out.println(cache.stats()); //获取统计信息
	}

}









