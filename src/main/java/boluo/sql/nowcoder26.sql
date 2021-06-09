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

-- 有一个职称表titles简况如下:
| emp_no |          title     | from_date |  to_date  |
|--------|--------------------|-----------|-----------|
| 10001  |   Senior Engineer  | 1986-06-26| 9999-01-01|
| 10002  |   Staff            | 1996-08-03| 9999-01-01|
| 10003  |   Senior Engineer  | 1995-12-03| 9999-01-01|

--汇总各个部门当前员工的title类型的分配数目,
-- 即结果给出部门编号dept_no、dept_name、
-- 其部门下所有的员工的title以及该类型title对应的数目count, 结果按照dept_no升序排序
| dept_no| dept_name |          title     |  count |
|--------|-----------|--------------------|--------|
|  d001  | Marketing |   Senior Engineer  |    1   |
|  d001  | Marketing |   Staff            |    1   |
|  d002  | Finance   |   Senior Engineer  |    1   |


select de.dept_no, d.dept_name, t.title, count(*) as count
from departments d
inner join dept_emp de on de.dept_no = d.dept_no
inner join titles t on de.emp_no = t.emp_no
group by de.dept_no, t.title, d.dept_no
order by de.dept_no asc




