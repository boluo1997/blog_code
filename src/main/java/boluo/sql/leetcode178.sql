-- 编写一个 SQL 查询来实现分数排名。
-- 如果两个分数相同，则两个分数排名（Rank）相同。请注意，平分后的下一个名次应该是下一个连续的整数值。换句话说，名次之间不应该有“间隔”。
+----+-------+
| id | score |
+----+-------+
| 1  | 3.50  |
| 2  | 3.65  |
| 3  | 4.00  |
| 4  | 3.85  |
| 5  | 4.00  |
| 6  | 3.65  |
+----+-------+
-- 例如，根据上述给定的 Scores 表，你的查询应该返回（按分数从高到低排列）：
+-------+------+
| score | rank |
+-------+------+
| 4.00  | 1    |
| 4.00  | 1    |
| 3.85  | 2    |
| 3.65  | 3    |
| 3.65  | 3    |
| 3.50  | 4    |
+-------+------+

-- 解题思路
-- 1. 先把分数降序排列,
-- 2. 计算每个分数对应的排名
select a.score as score
from scores a
order by a.score desc

-- 第二部分: 假设现在给你一个分数 X, 你怎么计算出该分数的排名
-- 先提取出大于等于该分数的集合 H, 去重之后的元素个数就是该分数的排名
select count(distinct b.score)
from scores b
where b.score >= x;

-- 第二部分接收的分数正是第一部分传过来的分数
select a.score,
(select count(distinct b.score) from scores b where b.score > a.score) as rank
from score a
order by a.score desc













