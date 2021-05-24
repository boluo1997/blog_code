-- 有一个员工表dept_emp简况如下:
| emp_no | dept_no | from_date |  to_date  |
|--------|---------|-----------|-----------|
| 10001  |   d001  | 1986-06-26| 9999-01-01|
| 10002  |   d002  | 1996-08-03| 9999-01-01|
| 10003  |   d003  | 1996-08-03| 9999-01-01|

-- 有一个薪水表salaries如下
| emp_no |  salary | from_date |  to_date  |
|--------|---------|-----------|-----------|
| 10001  |   88958 | 2002-06-22| 9999-01-01|
| 10002  |   72527 | 2001-08-02| 9999-01-01|
| 10003  |   92527 | 2001-08-02| 9999-01-01|

-- 获取每个部门中当前员工薪水最高的相关信息, 给出dept_no, emp_no以及其对应的salary,
-- 按照部门编号升序排列, 以上例子输出如下:
| dept_no|  emp_no | maxSalary |
|--------|---------|-----------|
|  d001  |   10001 |   88958   |
|  d002  |   10003 |   92527   |


select distinct dept_no, s.emp_no, salary
from dept_emp d
inner join salaries s on d.emp_no = d.emp_no
and salary = (
    select s2.salary
    from salaries s2
    inner join dept_emp d2 on s2.emp_no = d2.emp_no
    where d2.dept_no = d.dept_no
    order by s2.salary desc
    limit 1
)
order by dept_no


