--  case when
case sex
when '1' then '男'
when '2' then '女'
else '其他' end

-- case when search
case when sex = '1' then '男'
when sex = '2' then '女'
else '其他' end


-- 国家(country)	    人口(population)
-- 中国	            600
-- 美国	            100
-- 加拿大	        100
-- 英国	            200
-- 法国	            300
-- 日本	            250
-- 德国	            200
-- 墨西哥	        50
-- 印度	            250


-- 根据国家人口数据, 统计亚洲和北美洲人口数量, 应该得到以下数据
-- 洲	    人口
-- 亚洲	    1100
-- 北美洲	250
-- 其他	    700

select sum(population)
case country
when '中国' then '亚洲'
when '印度' then '亚洲'
when '日本' then '亚洲'
when '美国' then '北美洲'
when '加拿大' then '北美洲'
when '墨西哥' then '北美洲'
else '其他' end
from table_a
group by case country
when '中国' then '亚洲'
when '印度' then '亚洲'
when '日本' then '亚洲'
when '美国' then '北美洲'
when '加拿大' then '北美洲'
when '墨西哥' then '北美洲'
else '其他' end

-- 同样的, 我们也可以用这个方法来判断工资的等级, 并统计每一等级的人数
select
case when salary <= 500 then '1'
when salary > 500 and salary <= 600 then '2'
when salary > 600 and salary <= 800 then '3'
when salary > 800 and salary <= 1000 then '4'
else null end salary_class,
count(*)
from table_a
group by
case when salary <= 500 then '1'
when salary > 500 and salary <= 600 then '2'
when salary > 600 and salary <= 800 then '3'
when salary > 800 and salary <= 1000 then '4'
else null end


-- 用一个SQL语句完成不同条件的分组
-- 国家(country)	    性别(sex)	人口(population)
-- 中国	            1	        340
-- 中国	            2	        260
-- 美国	            1	        45
-- 美国	            2	        55
-- 加拿大	        1	        51
-- 加拿大	        2	        49
-- 英国	            1	        40
-- 英国	            2	        60


-- 按照国家和性别进行分组, 结果如下
-- 国家	    男	    女
-- 中国	    340	    260
-- 美国	    45	    55
-- 加拿大	51	    49
-- 英国	    40	    60

select country,
sum(case when sex = '1' then population else 0 end),
sum(case when sex = '2' then population else 0 end)
from table_a
group by country;

















