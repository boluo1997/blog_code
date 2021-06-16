-- 针对salaries表emp_no字段创建索引idx_emp_no, 查询emp_no为10005, 使用强制索引

CREATE TABLE `salaries`
(
    `emp_no`    int(11) NOT NULL,
    `salary`    int(11) NOT NULL,
    `from_date` date NOT NULL,
    `to_date`   date NOT NULL,
    PRIMARY KEY (`emp_no`, `from_date`)
);
create
index idx_emp_no on salaries(emp_no);

-- 使用强制索引
select *
from salaries
force index (idx_emp_no)
where emp_no = 10005;
