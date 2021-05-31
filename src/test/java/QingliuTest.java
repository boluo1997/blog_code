import boluo.work.Qingliu;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class QingliuTest {

	private static ObjectMapper mapper = new ObjectMapper();

	static SparkSession spark = SparkSession
			.builder()
			.master("local[*]")
			.appName("Simple Application")
			.getOrCreate();

	@Test
	public void publicTest() {
		Method[] declaredMethods = Qingliu.class.getDeclaredMethods();
		Set<String> publicMethod = Arrays.stream(declaredMethods).filter(i -> Modifier.isPublic(i.getModifiers()))
				.map(Method::getName)
				.collect(Collectors.toSet());
		Assert.assertEquals(ImmutableSet.of("replace"), publicMethod);
	}

	@Test
	public void replaceTest() {
		String str1 = "{\"编号\":20,\"申请人\":[{\"uid\":866359,\"name\":\"丁超\",\"email\":\"225cfa970732@ding.qingflow.com\",\"head\":\"https://static-legacy.dingtalk.com/media/lADPD3zUNDwJMTjNBBrNAlg_600_1050.jpg\"}],\"申请时间\":\"2021-05-31 11:59:34\",\"更新时间\":\"2021-05-31 11:59:34\",\"当前流程状态\":[\"已通过\"],\"测试字段\":\"boluo4\"}";
		String str2 = "{\"编号\":7,\"申请人\":[{\"uid\":651165,\"name\":\"小兰Robot\",\"email\":\"243e26d740a5@ding.qingflow.com\",\"head\":\"https://static-legacy.dingtalk.com/media/lADPDgtYuVWOhkzNA8DNA3Y_886_960.jpg\"}],\"申请时间\":\"2021-05-28 11:04:38\",\"更新时间\":\"2021-05-28 11:04:38\",\"当前流程状态\":[\"已通过\"],\"测试字段\":\"boluo\"}";

		List<String> list = ImmutableList.of(str1, str2);
		Dataset<String> df = SparkSession.active().createDataset(list, Encoders.STRING());
		Dataset<Row> ds = SparkSession.active().read().json(df);
		String key = "编号";
		Qingliu.replace(ds, null, key);

	}
}
