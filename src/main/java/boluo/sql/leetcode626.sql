-- 小美是一所中学的信息科技老师, 她有一张 seat 座位表, 平时用来储存学生名字和与他们相对应的座位 id。
-- 其中纵列的 id 是连续递增的, 小美想改变相邻俩学生的座位。
-- 示例：
+---------+---------+
|    id   | student |
+---------+---------+
|    1    | Abbot   |
|    2    | Doris   |
|    3    | Emerson |
|    4    | Green   |
|    5    | Jeames  |
+---------+---------+
-- 假如数据输入的是上表，则输出结果如下：
+---------+---------+
|    id   | student |
+---------+---------+
|    1    | Doris   |
|    2    | Abbot   |
|    3    | Green   |
|    4    | Emerson |
|    5    | Jeames  |
+---------+---------+

-- 解题思路:对于所有座位id是奇数的学生, 修改其id为id+1 (如果最后一个座位是奇数, 则不修改), 对于所有偶数位的学生, 修改其座位id为id-1

-- 1.首先查询座位数量
select count(*) as counts
from seat

-- 2.然后使用case条件和mod函数修改每个学生的座位id
select ( case
when mod(id, 2) != 0 and counts != id then id + 1
when mod(id, 2) != 0 and counts = id then id
else id - 1 end
) as id, student
from (
    seat, (
        select count(*) as counts
        from seat
    ) as seat_counts
)
order by id



