-- 表 Weather
+----+------------+-------------+
| id | recordDate | Temperature |
+----+------------+-------------+
| 1  | 2015-01-01 | 10          |
| 2  | 2015-01-02 | 25          |
| 3  | 2015-01-03 | 20          |
| 4  | 2015-01-04 | 30          |
+----+------------+-------------+
-- id 是这个表的主键 该表包含特定日期的温度信息
-- 编写一个 SQL 查询，来查找与之前（昨天的）日期相比温度更高的所有日期的 id 。
Result table:
+----+
| id |
+----+
| 2  |
| 4  |
+----+
-- 2015-01-02 的温度比前一天高（10 -> 25）
-- 2015-01-04 的温度比前一天高（20 -> 30）

select w1.id
from weather w1, weather w2
where w1.Temperature > w2.Temperature and datediff(d1.recordDate, d2.recordDate) = 1;