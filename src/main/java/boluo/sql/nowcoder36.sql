-- 对于如下表actor, 其对应的数据为:

| actor_id | first_name | last_name |    last_update     |
|----------|------------|-----------|--------------------|
|     1    | PENELOPE|  GUINESS     | 2006-02-15 12:34:33|
|     2    |   NICK  |  WAHLBERG    | 2006-02-15 12:34:33|


-- 请你创建一个actor_name表, 并且将actor表中的所有first_name以及last_name导入该表
-- actor_name表结构如下:
| 列表        | 类型       | 是否为null |  comment    |
|------------|------------|-----------|------------|
| first_name | varchar(45)|  not null |     名字    |
| last_name  | varchar(45)|  not null |     姓氏    |


-- 1.常规建表
create table if not exists table_1
(
    id varchar(45) not null;
)


-- 2.复制表格
create table_2 like table_1


-- 3.将table_1的一部分拿来创建新表格
create table if not exists actor_name
(
    first_name varchar(45) not null,
    last_name varchar(45) not null
)
select first_name, last_name
from actor



