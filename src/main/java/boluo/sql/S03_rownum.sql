-- 求用户的最大连续登录天数

-- 原始数据
user_id     login_date
U01         2019-10-10
U01         2019-10-11
U01         2019-10-12
U01         2019-10-14
U01         2019-10-15
U01         2019-10-17
U01         2019-10-18
U01         2019-10-19
U01         2019-10-20
U02         2019-10-20

-- 核心算法是先按时间排序, 登录时间减去排序后的序号, 得到一个日期, 按照这个日期分组计数即可
-- 1.排序
select user_id, login_date, row_number() over(partition by user_id order by login_date desc) as rank
from table_user

-- 结果
user_id     login_date      rank
U01         2019-10-10      1
U01         2019-10-11      2
U01         2019-10-12      3
U01         2019-10-14      4
U01         2019-10-15      5
U01         2019-10-17      6
U01         2019-10-18      7
U01         2019-10-19      8
U01         2019-10-20      9
U02         2019-10-20      1

-- 然后使第二列与第三列做日期差值
select user_id, date_sub(login_date, rank) dts
from (
    select user_id, login_date, row_number() over(partition by user_id order by login_date desc) as rank
    from table_user
) t

-- 然后再按时间分组求和即可
select user_id, count(1) as num
from (
    select user_id, date_sub(login_date, rank) dts
    from (
        select user_id, login_date, row_number() over(partition by user_id order by login_date desc) as rank
        from table_user
    ) t
    group by dts
)
where num = 7






















