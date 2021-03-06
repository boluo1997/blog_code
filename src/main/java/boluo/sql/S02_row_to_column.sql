y_order_info表(a表)中有如下信息, 其中oi_id是与y_order_info_param表的关联字段,只取其中的oi_date字段即可
+-------+--------+--------------------+----------------+---------------+---------------+---------+-------------+-----------------+--------------------+--------------------+-------------+-----------+------------+----------------+-------+---------+-------------------+
|  oi_id|oi_ci_id|           oi_number|oi_refund_number|oi_refund_price|oi_introduction|oi_source|oi_real_money|oi_original_money|           oi_pay_id|         oi_trade_id|oi_pay_status|oi_pay_type|oi_refund_id|oi_refund_status|oi_type|oi_delete|            oi_date|
+-------+--------+--------------------+----------------+---------------+---------------+---------+-------------+-----------------+--------------------+--------------------+-------------+-----------+------------+----------------+-------+---------+-------------------+
|5429830|    9367|ON155075422694332...|            null|           null|       加强洗涤|        3|         7.00|             7.00|27d7f316bd9b4a659...|20190221220014233...|       normal|     支付宝|        null|            null|   1300|        0|2019-02-21 21:03:46|
|5429831|       1|ON155075423285460...|            null|           null|       标准洗涤|        1|         5.00|             5.00|                null|                null|         null|       null|        null|            null|   1450|        0|2019-02-21 21:03:52|


y_order_info_param表(b表)
+---------+---------+---------------+-------------------+----------+-------------------+
|   oip_id|oip_oi_id|        oip_key|          oip_value|oip_delete|           oip_date|
+---------+---------+---------------+-------------------+----------+-------------------+
|187262926|  8467407|     start_date|2019-07-28 22:49:00|         0|2019-07-28 22:47:54|
|187262927|  8467407|       end_date|2019-07-28 23:40:00|         0|2019-07-28 22:47:54|
|187262928|  8467407|real_start_date|2019-07-28 22:48:16|         0|2019-07-28 22:47:54|
|187262929|  8467407|  real_end_date|2019-07-28 23:23:00|         0|2019-07-28 22:47:54|
|187262930|  8467407|         sii_id|                  2|         0|2019-07-28 22:47:54|
|187262931|  8467407|         spi_id|                220|         0|2019-07-28 22:47:54|
|187262932|  8467407|       spi_name|           标准洗涤|         0|2019-07-28 22:47:54|
|187262933|  8467407|         sti_id|                157|         0|2019-07-28 22:47:54|
|187262934|  8467407|       sti_name|        兰德力141店|         0|2019-07-28 22:47:54|
|187262935|  8467407|    sti_address| 浙大舟山校区男生楼|         0|2019-07-28 22:47:54|
|187262936|  8467407|         eti_id|                 38|         0|2019-07-28 22:47:54|
|187262937|  8467407|       eti_name|   浙江大学舟山校区|         0|2019-07-28 22:47:54|
|187262938|  8467407|    eti_address|          浙大路1号|         0|2019-07-28 22:47:54|
|187262939|  8467407|         cmb_id|              46065|         0|2019-07-28 22:47:54|
|187262940|  8467407|       plp_type|                  5|         0|2019-07-28 22:47:54|
|187262941|  8467407|      plp_price|               2.49|         0|2019-07-28 22:47:54|
|187262942|  8467407|       run_code|               5422|         0|2019-07-28 22:47:54|
|187262943|  8467407|         dii_id|                987|         0|2019-07-28 22:47:54|
|187262944|  8467407|     dii_number|                102|         0|2019-07-28 22:47:54|
|187262945|  8467407|       dii_type|                  1|         0|2019-07-28 22:47:54|
+---------+---------+---------------+-------------------+----------+-------------------+

select bb.stiid, bb.stiname, a.oi_date
from a
right join
(
    select b.oip_oi_id,
    max(if(b.oip_key = 'sti_id', oip_value, null)) as stiid,
    max(if(b.oip_key = 'sti_name', oip_value, null)) as stiname
    from b
    where b.oip_key = 'sti_id' or b.oip_key = 'sti_name'
    group by b.oip_oi_id
) bb on bb.oip_oi_id = a.oi_id

































