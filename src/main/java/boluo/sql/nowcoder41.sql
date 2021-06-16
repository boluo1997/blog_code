-- 构造一个触发器audit_log, 在向employees_test表中插入一条数据的时候, 触发插入相关的数据到audit中
CREATE TABLE employees_test
(
    ID      INT PRIMARY KEY NOT NULL,
    NAME    TEXT            NOT NULL,
    AGE     INT             NOT NULL,
    ADDRESS CHAR(50),
    SALARY  REAL
);

CREATE TABLE audit
(
    EMP_no INT  NOT NULL,
    NAME   TEXT NOT NULL
);

-- 构造一个触发器
create trigger audit_log after insert
on employees_test
for each row
begin
    insert into audit values (new.id, new.name);
end


-- 【创建触发器】

-- 在MySQL中, 创建触发器语法如下：
-- CREATE TRIGGER trigger_name trigger_time trigger_event
-- ON tbl_name FOR EACH ROW trigger_stmt

-- 其中：
-- trigger_name: 标识触发器名称, 用户自行指定
-- trigger_time: 标识触发时机, 取值为 BEFORE 或 AFTER
-- trigger_event: 标识触发事件, 取值为 INSERT、UPDATE 或 DELETE
-- tbl_name: 标识建立触发器的表名, 即在哪张表上建立触发器
-- trigger_stmt: 触发器程序体, 可以是一句SQL语句, 或者用 BEGIN 和 END 包含的多条语句

-- 由此可见, 可以建立6种触发器, 即:
-- BEFORE INSERT、AFTER INSERT
-- BEFORE UPDATE、AFTER UPDATE
-- BEFORE DELETE、AFTER DELETE






