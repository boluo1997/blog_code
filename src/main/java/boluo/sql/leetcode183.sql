-- 某网站包含两个表, Customers 表和 Orders 表。编写一个 SQL 查询, 找出所有从不订购任何东西的客户。
+----+-------+
| Id | Name  |
+----+-------+
| 1  | Joe   |
| 2  | Henry |
| 3  | Sam   |
| 4  | Max   |
+----+-------+

+----+------------+
| Id | CustomerId |
+----+------------+
| 1  | 3          |
| 2  | 1          |
+----+------------+

select Name as CustomerId
from Customers
where id not in (
    select Id
    from orders
)