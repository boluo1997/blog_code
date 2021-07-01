package boluo.basics;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import boluo.basics.Note06_ListStream.User;

/**
 * Stream
 */
public class Note07_Stream {

	public static final ObjectMapper mapper = new ObjectMapper();

	@Test
	public void func1() {
		// List转Stream
		List<String> list = Lists.newArrayList();
		list.add("dingc");
		Stream stream = list.stream();
		stream.forEach(System.out::println);
	}

	@Test
	public void func2() {
		// 数组转Stream
		String[] arr = new String[]{"boluo", "dingc", "qidai"};
		Stream stream = Arrays.stream(arr);
		stream.forEach(System.out::println);
	}

	@Test
	public void func3() {
		// Map转Stream
		// Map不是一个序列, 不是集合, 没办法直接转成stream(), 但entrySet()是Set, 可以转
		Map<String, Integer> map = Maps.newHashMap();
		map.put("boluo", 20);
		Stream stream1 = map.keySet().stream();
		Stream stream2 = map.values().stream();
		Stream stream3 = map.entrySet().stream();
		stream1.forEach(System.out::println);
		stream2.forEach(System.out::println);
		stream3.forEach(System.out::println);
	}

	@Test
	public void func4() {
		// 直接创建Stream
		Stream stream1 = Stream.of("boluo", 20);
		stream1.forEach(System.out::println);

		// Stream提供了iterate来生成一个无限序列, 一个基于初始值的无限序列, 可以用lambda设置序列的生成规则, 比如每次增加2
		Stream.iterate(0, n -> n + 2).limit(10).forEach(System.out::println);

		// 斐波那契数列
		Stream.iterate(new int[]{0, 1}, t -> new int[]{t[1], t[0] + t[1]})
				.limit(20)
				.map(t -> t[0])
				.forEach(System.out::println);

		// Stream还提供了另一个generate方法来生成序列。接收一个用户指定的生成序列函数IntSupplier.
		IntSupplier fib = new IntSupplier() {
			private int previous = 0;
			private int current = 1;

			@Override
			public int getAsInt() {
				int oldPrevious = this.previous;
				int nextValue = this.previous + this.current;
				this.previous = this.current;
				this.current = nextValue;
				return oldPrevious;
			}
		};
		IntStream.generate(fib).limit(10).forEach(System.out::println);

		String str = Stream.generate(() -> "?").limit(5).collect(Collectors.joining(","));
		System.out.println(str);
	}

	@Test
	public void func5() {

		List<User> list = Lists.newArrayList();

		list.add(new User("boluo", 20));
		list.add(new User("qidai", 40));
		list.add(new User("dingc", 30));
		System.out.println(list);

		// filter
		List<User> list2 = list.stream()
				.filter(user -> {
					return user.age > 20;
				}).collect(Collectors.toList());
		System.out.println(list2);

		// map
		List<Integer> list3 = list.stream()
				.map(user -> {
					return user.age += 5;
				}).collect(Collectors.toList());
		System.out.println(list3);

		// mapToInt
		Integer sumAge = list3.stream().mapToInt(i -> i - 5).sum();
		Optional<Integer> sumAge2 = list3.stream().reduce(Integer::sum);
		System.out.println(sumAge);
		System.out.println("sumAge2: " + sumAge2);

		// sorted
		List<User> list4 = list.stream().sorted((a, b) -> {
			return a.getAge() - b.getAge();
		}).collect(Collectors.toList());
		System.out.println(list4);

		// forEach

		// Optional min(Comparator comparator)

		// anyMatch
		boolean br = list.stream().anyMatch(user -> {
			return user.getAge() == 25;
		});
		System.out.println(br);

		// distinct
		list3.add(25);
		List<Integer> list5 = list3.stream().distinct().collect(Collectors.toList());
		System.out.println(list5);
	}

	@Test
	public void func6() {
		// 缩减操作
		// 最终将流缩减为一个值的终端操作, 称之为缩减操作. 上例中提到的 min(), max()方法返回的流中的最小或最大值,
		// 这两个方法属于特例缩减操作. 而通用的缩减操作就是指的 reduce()方法

		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
		Optional<Integer> count = list.stream().reduce((a, b) -> (a + b));
		System.out.println(count.get());


		Integer count1 = list.stream().reduce(2, (a, b) -> (a * b));
		System.out.println(count1);
	}

	@Test
	public void func7() {
		// 之前两种签名返回的数据只能和Stream流中的元素类型一致, 第三种可以不一致
		List<Integer> list = Arrays.asList(Integer.MAX_VALUE, Integer.MAX_VALUE);
		long count = list.stream().reduce(0L, (a, b) -> (a + b), (a, b) -> 0L);
		System.out.println(count);

		// 总的来说, 缩减操作有两个特点, 一是只返回一个值, 二是它是一个终端操作.
	}

	@Test
	public void func8() {
		List<Student> list1 = Lists.newArrayList();
		list1.add(new Student("boluo", ImmutableList.of(66.6, 77.7, 88.8)));
		list1.add(new Student("dingc", ImmutableList.of(88.8, 99.9)));
		list1.add(new Student("qidai", ImmutableList.of(99.9, 100.0)));
		// 映射, 将一个集合转换成另外一个对象的集合.
		// 映射操作主要是将一个Stream流转换成另外一个对象的Stream流或者将一个Stream流中符合条件的元素放到一个新的Stream流中

		// flatMap() 把原始流中的元素进行一对多的转换, 并且将新生成的元素全部合并到它返回的流中
		List<Double> list2 = list1.stream()
				.flatMap(one -> one.getScore().stream().distinct())
				.collect(Collectors.toList());
		System.out.println(list2);
	}

	@Test
	public void func9() {
		// 收集操作 Collector是一个收集器, 指定收集过程如何执行, collect()是一个终端方法
		// 使用收集操作将List转成Map
		List<Student> list = Lists.newArrayList();
		list.add(new Student("boluo", ImmutableList.of(99.9, 77.7, 88.8)));
		list.add(new Student("dingc", ImmutableList.of(66.9, 88.8)));
		list.add(new Student("qidai", ImmutableList.of(99.9, 100.0)));

		Map<String, List<Double>> map = list.stream()
				.collect(Collectors.toMap(one -> one.getName(), Student::getScore));
		System.out.println(map);
	}

	@Test
	public void func10() {

		// map and flatMap
		String[] words = new String[]{"Hello", "World"};
		List<String[]> a1 = Arrays.stream(words)
				.map(word -> word.split(""))
				.collect(Collectors.toList());
		a1.forEach(System.out::println);
		a1.forEach(jn -> Arrays.stream(jn).collect(Collectors.toList()).forEach(System.out::println));

		List<String> a2 = Arrays.stream(words)
				.map(word -> word.split(""))
				// .flatMap(word ->Stream.of(word))
				.flatMap(Stream::of)

				// .flatMap(word -> Arrays.stream(word.clone()))
				// .flatMap(Arrays::stream)

				// .flatMap(word -> Stream.of(word.split("")))
				.collect(Collectors.toList());
		a2.forEach(System.out::print);
	}

	class Student {
		private String name;
		private List<Double> score;

		public Student(String name, List<Double> score) {
			this.name = name;
			this.score = score;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<Double> getScore() {
			return score;
		}

		public void setScore(List<Double> score) {
			this.score = score;
		}
	}


	@Test
	public void func11() {
		ArrayNode answer1 = mapper.createArrayNode();
		answer1.add(mapper.createObjectNode().put("queId", 1));
		answer1.add(mapper.createObjectNode().put("queId", 2));
		answer1.add(mapper.createObjectNode().put("queId", 3));
		answer1.add(mapper.createObjectNode().put("queId", 4));

		// 过滤掉answer1中queId为奇数的数据
		// JDK原生
		ArrayNode answer2 = mapper.createArrayNode();
		StreamSupport.stream(answer1.spliterator(), false)
				.filter(i -> i.at("/queId").asInt() % 2 == 0)
				.forEach(answer2::add);

		System.out.println(answer2);

	}

	@Test
	public void func12() {
		ObjectNode node = mapper.createObjectNode();
		node.put("queId", 4)
				.put("queTitle", "title")
				.put("queType", 18);

		ArrayNode tableValues = node.withArray("tableValues");
		ArrayNode values1 = mapper.createArrayNode();
		ArrayNode values2 = mapper.createArrayNode();

		ObjectNode value1 = mapper.createObjectNode()
				.put("queId", 41)
				.put("qeuType", 8);
		value1.withArray("values").add(mapper.createObjectNode().put("value", "a41").put("ordinal", 1));

		ObjectNode value2 = mapper.createObjectNode()
				.put("queId", 41)
				.put("qeuType", 8);
		value2.withArray("values").add(mapper.createObjectNode().put("value", "b41").put("ordinal", 2));

		values1.add(value1);
		values2.add(value2);

		tableValues.add(values1);
		tableValues.add(values2);

		// 找到tableValues中行数为2的values, 也就是values2
		List<JsonNode> list = Streams.stream(node.at("/tableValues")).filter(tableValue -> {
			// tableValue还是一个数组
			return Streams.stream(tableValue).flatMap(jn -> Streams.stream(jn.at("/values")))
					.anyMatch(i -> i.at("/ordinal").asInt() == 2);
		}).collect(Collectors.toList());

		System.out.println(list);
	}

}






