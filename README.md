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
    $sql = 'select * from alp_dish_sales_saas where sid in(994,290) limit 1,10';
    //$sql='update alp_dish_sales_saas set mid=3  where adsid=15125110';
    //$sql='delete from alp_dish_sales_saas where adsid=15546509';
    $es_config=array(
	    'index' =>"alp_dish_sales_saas",
	    'type'  =>"alp_dish_sales_saas",
	    'url'   =>"http://127.0.0.1:9200"
	 );
    $start = microtime(true);
    $parser = new EsParser($sql, true,$es_config);//第三个参数是es的配置参数，一定要配置
    $stop = microtime(true);
    print_r($parser->result);//打印结果
    //print_r($parser->explain());//打印dsl
### 普通调用
	require_once dirname(__FILE__) . '/src/library/EsParser.php';
	$sql = 'select * from alp_dish_sales_saas where sid in(994,290) limit 1,10';
    	//$sql='update alp_dish_sales_saas set mid=3  where adsid=15125110';
    	//$sql='delete from alp_dish_sales_saas where adsid=15546509';
    	$es_config=array(
        	'index' =>"alp_dish_sales_saas",
        	'type'  =>"alp_dish_sales_saas",
        	'url'   =>"http://127.0.0.1:9200"
    	);
    	$start = microtime(true);
    	$parser = new EsParser($sql, true,$es_config);//第三个参数是es的配置参数，一定要配置
    	$stop = microtime(true);
    	print_r($parser->result);//打印结果
    	//print_r($parser->explain()); //打印dsl
### 目前支持的sql函数
    *  SQL Select
    *  SQL Delete
    *  SQL Update
    *  SQL Where
    *  SQL Order By
    *  SQL Group By
    *  SQL AND 
    *  SQL Like
    *  SQL COUNT distinct
    *  SQL In
    *  SQL avg()
    *  SQL count()
    *  SQL max()
    *  SQL min()
    *  SQL sum()
### 使用注意事项
    该版本是基于elasticsearch5.x以上开发的，elasticsearch2.x及其以下不支持
### 交流使用
    qq群：578276199
### 项目地址
    github：https://github.com/qieangel2013/EsParser
    oschina：https://gitee.com/qieangel2013/EsParser
