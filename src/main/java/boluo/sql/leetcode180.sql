-- 编写一个 SQL 查询，查找所有至少连续出现三次的数字。
-- Logs 表：
+----+-----+
| Id | Num |
+----+-----+
| 1  | 1   |
| 2  | 1   |
| 3  | 1   |
| 4  | 2   |
| 5  | 1   |
| 6  | 2   |
| 7  | 2   |
+----+-----+
-- Result 表：
+-----------------+
| ConsecutiveNums |
+-----------------+
| 1               |
+-----------------+


-- 题解任意连续n问题 -- 构造新属性 id-rownum
-- 构造新属性rownum, 且rownum所实现的需求为分组排序
select *, row_number() over(partition by num order by id) rownum
from logs

-- 执行之后, 会变成:
+----+-----+------+
| Id | Num |rownum|
+----+-----+------+
| 1  | 1   |   1  |
| 2  | 1   |   2  |
| 3  | 1   |   3  |
| 4  | 2   |   1  |
| 5  | 1   |   4  |
| 6  | 2   |   2  |
| 7  | 2   |   3  |
+----+-----+------+

-- 从这里可以看出id是恒大于rownum的, 因此我们可以构建一个变量id-rownum
select *, id-rownum
from (
    select *, row_number() over(partition by num order by id) rownum
    from logs
) t

-- 执行之后, 会变成:
+----+-----+------+-----------+
| Id | Num |rownum| id-rownum |
+----+-----+------+-----------+
| 1  | 1   |   1  |     0     |
| 2  | 1   |   2  |     0     |
| 3  | 1   |   3  |     0     |
| 4  | 2   |   1  |     3     |
| 5  | 1   |   4  |     1     |
| 6  | 2   |   2  |     4     |
| 7  | 2   |   3  |     4     |
+----+-----+------+-----------+

-- 由上图结果可以看出, 当数字是连续出现时, id-rownum的值是一致的, 我们可以利用这一特性来判断当前数字的最大连续次数
-- 进行分组查询
select distinct num ConsecutiveNums
from (
    select *, row_number() over(partition by num order by id) rownum
    from logs
) t
group by (id-rownum), num
having count(*) >= 3

-- id可能为0或不连续, 所以我们可以自己构造一个id2来达到单调且连续的效果
row_number() over(order by id) id2

-- 结果为:
select distinct num ConsecutiveNums
from (
    select *,
    row_number() over(partition by num order by id) rownum,
    row_number() over(order by id) id
    from logs
) t
group by (id2-rownum), num
having count(*) >= 3















