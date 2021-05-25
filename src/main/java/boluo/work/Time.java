package boluo.work;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;

/**
 * 一些关于时间的常用方法
 */
public class Time {
    public static void main(String[] args) {

    }

    @Test
    // 字符串转LocalDate类型的时间
    public void func1() {
        String timeStr = "2021-05-03";
        LocalDate startTime = LocalDate.parse(timeStr);
        System.out.println(startTime);
    }

    @Test
    // 通过时间进行循环
    public void func2() {
        String timeStr = "2021-05-03";
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate startTime = LocalDate.parse(timeStr, format);
        LocalDate endTime = LocalDate.parse("2021-05-05");

        for (LocalDate start = startTime; start.compareTo(endTime) < 0; start = start.plusDays(1)) {
            System.out.println(start);
        }
    }

    @Test
    public void func3() {
        // 指定一个特定的Timestamp时间
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2020, 1, 1, 0, 0));

        // Timestamp转时间戳
        long unix_timestamp = ts.getTime() / 1000;

        // 时间戳转LocalDate格式
        LocalDate startTime = Instant.ofEpochSecond(unix_timestamp).atZone(ZoneOffset.ofHours(8)).toLocalDate();

        // 时间戳转Timestamp
        Instant instant = Instant.ofEpochMilli(unix_timestamp);
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDateTime tempLocalDateTime = LocalDateTime.ofInstant(instant, zoneId);
        Timestamp timestamp = Timestamp.valueOf(tempLocalDateTime);

        // 创建固定时刻的Instant
        Instant newInstant = Timestamp.valueOf(LocalDateTime.of(1997, 3, 1, 12, 30)).toInstant();
    }

    @Test
	// 按月遍历时间
    public void func4(){
        String startTimeStr = "2020-01-01";
        String endTimeStr = "2021-05-25";

        LocalDate startTime = LocalDate.parse(startTimeStr);
        LocalDate endTime = LocalDate.parse(endTimeStr);

        int k = 1;
        for (LocalDate start = startTime; start.compareTo(endTime) < 0; start = startTime.plusMonths(k++)) {
			LocalDate end = Stream.of(start.plusMonths(1), endTime).min(LocalDate::compareTo).get();
			System.out.println(RowFactory.create(start, end));
		}
    }

}













