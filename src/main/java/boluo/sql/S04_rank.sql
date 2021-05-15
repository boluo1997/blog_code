-- 计算除去部门最高工资, 和最低工资的平均工资
-- 核心算法是使用窗口函数降序和升序分别排一遍取出最高和最低

select a.dept_no, avg(a.salary)
from (
    select *,
    rank() over(partition by dept_no order by salary) as rank_1
    rank() over(partition by dept_no order by salary desc) as rank_2
    from emp
) a
group by a.dept_no
where a.rank_1 > 1 and a.rank_2 > 1

