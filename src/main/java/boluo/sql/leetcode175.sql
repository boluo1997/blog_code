-- person
+-------------+---------+
| 列名         | 类型    |
+-------------+---------+
| PersonId    | int     |
| FirstName   | varchar |
| LastName    | varchar |
+-------------+---------+

-- address
+-------------+---------+
| 列名         | 类型    |
+-------------+---------+
| AddressId   | int     |
| PersonId    | int     |
| City        | varchar |
| State       | varchar |
+-------------+---------+

-- 编写一个 SQL 查询，满足条件：无论 person 是否有地址信息，都需要基于上述两表提供 person 的以下信息：
-- FirstName, LastName, City, State


select Person.FirstName,Person.LastName,Address.City,Address.State
from Person
left join Address on Person.PersonId = Address.PersonId