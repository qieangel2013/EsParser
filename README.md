# EsParser
php的操作类库，通过写sql来转化dsl来查询elasticsearch
### composer使用
    {
        "require": {
            "qieangel2013/esparser": "dev-master"
        }
    }
    composer install
    require __DIR__.'/vendor/autoload.php';
    //$sql = 'select * from alp_dish_sales_saas where sid in(994,290) limit 1,10';
    //$sql='update alp_dish_sales_saas set mid=3  where adsid=15125110';
    //$sql='delete from alp_dish_sales_saas where adsid=15546509';
    //$sql="select *,concat_ws('_',category_name.keyword,dish_name.keyword,sku_name.keyword) as dfg from alp_dish_sales_saas where sale_date>'2017-01-01' and sale_date<'2017-09-02' group by dfg order by total_count desc";
    $sql = 'select *,DATE_FORMAT(sale_date,"%Y-%m-%d") as days from alp_dish_sales_saas group by days ';
    $es_config=array(
	    'index' =>"alp_dish_sales_saas",
	    'type'  =>"alp_dish_sales_saas",
	    'url'   =>"http://127.0.0.1:9200",
        'version' =>"5.x" //1.x 2.x 5.x 6.x,可以不配置，系统会请求获取版本，这样会多一次请求,建议配置一下
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
    //print_r($parser->explain());//打印dsl
### 普通调用
	require_once dirname(__FILE__) . '/src/library/EsParser.php';
	//$sql = 'select * from alp_dish_sales_saas where sid in(994,290) limit 1,10';
	//$sql='update alp_dish_sales_saas set mid=3  where adsid=15125110';
	//$sql='delete from alp_dish_sales_saas where adsid=15546509';
    //$sql="select *,concat_ws('_',category_name.keyword,dish_name.keyword,sku_name.keyword) as dfg from alp_dish_sales_saas where sale_date>'2017-01-01' and sale_date<'2017-09-02' group by dfg order by total_count desc";
    $sql = 'select *,DATE_FORMAT(sale_date,"%Y-%m-%d") as days from alp_dish_sales_saas group by days ';
	$es_config=array(
        	'index' =>"alp_dish_sales_saas",
        	'type'  =>"alp_dish_sales_saas",
        	'url'   =>"http://127.0.0.1:9200",
            'version' =>"5.x" //1.x 2.x 5.x 6.x,可以不配置，系统会请求获取版本，这样会多一次请求,建议配置一下
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
### 目前支持的sql函数
    *  SQL Select
    *  SQL Delete
    *  SQL Update
    *  SQL Where
    *  SQL Order By
    *  SQL Group By
    *  SQL AND 
    *  SQL OR (多重or如:((a=1 and b=2) or (c=3 and d=4)) and e=5)
    *  SQL Like
    *  SQL Not Like
    *  SQL Is NULL
    *  SQL Is Not NULL
    *  SQL COUNT distinct
    *  SQL In
    *  SQL Not In
    *  SQL =
    *  SQL !=
    *  SQL <>
    *  SQL avg()
    *  SQL count()
    *  SQL max()
    *  SQL min()
    *  SQL sum()
    *  SQL Between
    *  SQL Aliases
    *  SQL concat_ws
    *  SQL DATE_FORMATE
    *  SQL Having
### 使用注意事项
    请在配置项填写es的版本,这样系统不会请求获取版本，这样不会多一次请求,建议配置一下
### 交流使用
    qq群：578276199
### 项目地址
    github：https://github.com/qieangel2013/EsParser
    oschina：https://gitee.com/qieangel2013/EsParser
### 如果你对我的辛勤劳动给予肯定，请给我捐赠，你的捐赠是我最大的动力
![](https://github.com/qieangel2013/zys/blob/master/public/images/pw.jpg)
![](https://github.com/qieangel2013/zys/blob/master/public/images/pay.png)
[项目捐赠列表](https://github.com/qieangel2013/zys/wiki/%E9%A1%B9%E7%9B%AE%E6%8D%90%E8%B5%A0)
