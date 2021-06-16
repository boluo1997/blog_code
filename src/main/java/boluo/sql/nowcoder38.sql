-- 针对actor表创建视图actor_name_view, 只包含first_name以及last_name两列, 并对这两列重新命名,
-- first_name为first_name_v, last_name修改为last_name_v:

CREATE TABLE actor
(
    actor_id    smallint(5) NOT NULL PRIMARY KEY,
    first_name  varchar(45) NOT NULL,
    last_name   varchar(45) NOT NULL,
    last_update datetime    NOT NULL
);


-- 创建视图

create view actor_name_view as
select first_name as first_name_v, last_name as last_name_v
from actor;

