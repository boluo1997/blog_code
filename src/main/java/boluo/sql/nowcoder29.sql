-- 使用join查询方式找出没有分类的电影id以及名称

-- film表
-- 字段	        说明
-- film_id	    电影id
-- title	    电影名称
-- description	电影描述信息

-- film_category表
-- 字段	        说明
-- film_id	    电影id
-- category_id	电影分类id
-- last_update	电影id和分类id对应关系的最后更新时间

-- join + not in
select f.film_id as 电影id, f.title as 名称
from film f
where f.film_id not in (
    select f.film_id
    from film f
    inner join film_category fc on fc.film_id = f.film_id
)

-- join + is null
select f.film_id as 电影id, f.title as 名称
from film f
left join film_category fc on fc.film_id = f.film_id
where fc.category_id is null

