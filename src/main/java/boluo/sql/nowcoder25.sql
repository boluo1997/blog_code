-- 有一个部门员工关系表dept_emp简况如下:
| emp_no | dept_no | from_date |  to_date  |
|--------|---------|-----------|-----------|
| 10001  |   d001  | 1986-06-26| 9999-01-01|
| 10002  |   d001  | 1996-08-03| 9999-01-01|

-- 有一个部门经理表dept_manager简况如下
| dept_no| emp_no  | from_date |  to_date  |
|--------|---------|-----------|-----------|
|  d001  |  10002  | 1986-06-26| 9999-01-01|

-- 有一个薪水表salaries如下
| emp_no |  salary | from_date |  to_date  |
|--------|---------|-----------|-----------|
| 10001  |   88958 | 2002-06-22| 9999-01-01|
| 10002  |   72527 | 1996-08-03| 9999-01-01|

-- 获取员工其当前的薪水比其manager当前薪水还高的相关信息
-- 以上例子输出如下:
| emp_no | manager_no | emp_salary | manager_salary |
|--------|------------|------------|----------------|
| 10001  |    10002   |   88958    |     72527      |



select a.emp_no, b.emp_no as manager_no, c.salary as emp_salary, d.salary as manager_salary
from dept_emp a
inner join dept_manager b on a.dept_no = b.dept_no
inner join salaries c on a.emp_no = c.emp_no
inner join salaries d on b.emp_no = d.emp_no
where c.salary > d.salary

