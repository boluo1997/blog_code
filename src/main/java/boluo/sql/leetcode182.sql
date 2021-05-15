-- 查找person表中所有重复的电子邮箱
+----+---------+
| Id | Email   |
+----+---------+
| 1  | a@b.com |
| 2  | c@d.com |
| 3  | a@b.com |
+----+---------+

-- 返回
+---------+
| Email   |
+---------+
| a@b.com |
+---------+

select Email
from person
group by Email
having count(Email) > 1;