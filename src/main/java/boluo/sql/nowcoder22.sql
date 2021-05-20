-- 有一个部门表departments简况如下:
| dept_no| dept_name |
|--------|-----------|
|  d001  | Marketing |
|  d002  | Finance   |

-- 有一个部门员工关系表dept_emp简况如下:
| emp_no | dept_no | from_date |  to_date  |
|--------|---------|-----------|-----------|
| 10001  |   d001  | 1986-06-26| 9999-01-01|
| 10002  |   d002  | 1996-08-03| 9999-01-01|
| 10003  |   d003  | 1996-08-03| 9999-01-01|

-- 有一个薪水表salaries如下
| emp_no |  salary | from_date |  to_date  |
|--------|---------|-----------|-----------|
| 10001  |   85097 | 2001-06-22| 2002-06-22|
| 10001  |   88958 | 2002-06-22| 9999-01-01|
| 10002  |   72527 | 1996-08-03| 9999-01-01|
| 10003  |   32323 | 1996-08-03| 9999-01-01|

-- 请你统计各个部门的工资记录数，给出部门编码dept_no、部门名称dept_name以及部门在salaries表里面有多少条记录sum，
-- 按照dept_no升序排序，以上例子输出如下:
| dept_no| dept_name | sum |
|--------|-----------|-----|
|  d001  | Marketing |  3  |
|  d002  | Finance   |  1  |




select b.dept_no, c.dept_name, count(a.salary) as sum
from salaries a
left join dept_emp b on a.emp_no = b.emp_no
left join departments c on b.dept_no = c.dept_no
group by b.dept_no, c.dept_name
order by b.dept_no
