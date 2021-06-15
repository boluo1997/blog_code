-- 对first_name创建唯一索引uniq_idx_firstname,
-- 对last_name创建普通索引idx_lastname

CREATE TABLE actor
(
    actor_id    smallint(5) NOT NULL PRIMARY KEY,
    first_name  varchar(45) NOT NULL,
    last_name   varchar(45) NOT NULL,
    last_update datetime    NOT NULL
);


-- 添加主键
alter table table_1 add primary key (id);

-- 添加唯一索引
alter table table_2 add unique index_name1 (col_name);

-- 添加普通索引
alter table table_3 add index index_name2 (col_name);

-- 添加全文索引
alter table table_4 add fulltext index_name3 (col_name);

-- 删除索引
drop index index_name2 on table_3;




