<?php
require_once dirname(__FILE__) . '/src/library/EsParser.php';
//$sql = 'select a.*,count(a.id) as id,sum(a.price) as total_price,sum(a.total) as count_total from table1 a where a.a=12 and a.b=36 and a.c like "%5%" and a.d>=10 and a.d<20 and a.h>56 and a.f in(1,2,3,4,5) group by a.name order by a.id desc limit 10';
//$sql='select a.*,count(a.id) as sid,a.total_price,sum(a.total) as count_total from table1 group by a.total_price order by count_total,a.co';
//$sql = 'select * from alp_dish_sales_saas where sid in(994,290) limit 0,10';
$sql='select category_name cate_name,dish_name,dishsno,sale_date,sum(total_price) total_price,sum(total_count) total_count,category_name,sku_name properties from alp_dish_sales_saas where sale_date>="2017-01-01" and sale_date<="2017-09-03" and sid in(994,290) limit 3,10';
//$sql='update alp_dish_sales_saas set mid=3  where adsid=15125110';
//$sql='delete from alp_dish_sales_saas where adsid=15546509';
$es_config=array(
	'index' =>"alp_dish_sales_saas",
	'type'  =>"alp_dish_sales_saas",
	'url'   =>"http://127.0.0.1:9200",
	'version' =>"5.x" //1.x 2.x 5.x 6.x,可以不配置，系统会请求获取版本，这样会多一次请求
);
$parser = new EsParser($sql, true,$es_config);//第三个参数是es的配置参数，一定要配置
print_r($parser->build());//打印结果
$result=$parser->scroll();//深度分页初始化会返回第一条
$result=json_decode($result,true);
print_r($result);//打印深度分页结果
$result1=$parser->scroll($result['scrollid']);//深度分页下一页
print_r(json_decode($result1,true));//打印深度分页结果
$result2=$parser->scroll($result['scrollid']);//深度分页下一页
print_r(json_decode($result2,true));//打印深度分页结果
$result3=$parser->scroll($result['scrollid']);//深度分页下一页
print_r(json_decode($result3,true));//打印深度分页结果
//print_r($parser->explain()); //打印dsl
