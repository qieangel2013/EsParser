<?php
/*
|---------------------------------------------------------------
|  Copyright (c) 2018
|---------------------------------------------------------------
| 作者：qieangel2013
| 联系：qieangel2013@gmail.com
| 版本：V1.0
| 日期：2018/3/13
|---------------------------------------------------------------
*/
require_once dirname(__FILE__) . '/positions/PositionCalculator.php';
require_once dirname(__FILE__) . '/processors/DefaultProcessor.php';
class EsParser {
    public $parsed;
    private $Builderarr;
    public $url;
    private $top_hits=0;
    private $top_hits_size=1;
    private $agg;
    private $havingagg=array();
    private $sort;
    private $index_es='';
    private $type_es='';
    private $version_es='';
    private $count_tmp=0;
    private $count_tmp_filter=0;
    private $count_tmp_range=0;
    private $count_fi=0;
    private $count_tmp_have=0;
    private $count_tmp_filter_have=0;
    private $count_tmp_range_have=0;
    private $count_fi_have=0;
    private $arrtmp=array();
    private $tmp_str='';
    private $tmp_str_filter='';
    private $tmp_fi='';
    private $tmp_str_range='';
    private $tmp_lock='';
    private $tmp_lock_str='';
    private $tmp_or=0;
    private $tmp_and=0;
    private $tmp_lock_fi='';
    private $tmp_lock_range='';
    private $tmp_str_have='';
    private $tmp_str_filter_have='';
    private $tmp_fi_have='';
    private $tmp_str_range_have='';
    private $tmp_lock_have='';
    private $tmp_lock_str_have='';
    private $tmp_lock_fi_have='';
    private $tmp_lock_range_have='';
    private $fistgroup='';
    private $limit;
    public $result;
    public $explain;
    public $build;
    private $scrolltime='3m';
    private $scrollurl='';
    private $basescrollurl='';
    private $isscroll=0;
    public $scroll;
    /**
     * Constructor. It simply calls the parse() function. 
     * Use the public variable $parsed to get the output.
     * 
     * @param String  $sql           The SQL statement.
     * @param boolean $calcPositions True, if the output should contain [position], false otherwise.
     */
    public function __construct($sql = false,$calcPositions = false,$es_config=array()) {
        if(empty($es_config)){
            $config_err=array(
                'fail' =>1,
                'message'=>'es的配置项不能为空!'
                 );
            $this->result=json_encode($config_err,true);
            return $this->result;
        }else{
            $this->index_es=$es_config['index'];
            $this->type_es=$es_config['type'];
            $this->url=$es_config['url'];
            $this->scrollurl=$es_config['url'];
            $this->basescrollurl=$es_config['url'];
            if(!isset($es_config['version'])){
                $version=$this->getEsData($es_config['url']);
                if($version){
                    if(version_compare($version,'5.0.0', '<')){
                        $this->version_es='2.x';
                    }else if( version_compare($version,'5.0.0', '>=') && version_compare($version,'6.0.0', '<')){
                        $this->version_es='5.x';
                    }else if( version_compare($version,'6.0.0', '>=') && version_compare($version,'7.0.0', '<')){
                        $this->version_es='6.x';
                    } else {
                        $this->version_es='7.x';
                    }
                }else{
                    $this->version_es='5.x';
                }
            }else{
                if(trim($es_config['version'])==''){
                    $this->version_es='5.x';
                }else{
                    $this->version_es=$es_config['version'];
                }
            }
            
        }
        if ($sql) {
            $this->parse($sql, $calcPositions);
        }
    }

    /**
     * 
     * @param String  $sql           The SQL statement.
     * @param boolean $calcPositions True, if the output should contain [position], false otherwise.
     * 
     * @return array An associative array with all meta information about the SQL statement.
     */
    
    public function parsesql($sql, $calcPositions = false) {
        $processor = new DefaultProcessor();
        $queries = $processor->process($sql);
        if ($calcPositions) {
            $calculator = new PositionCalculator();
            $queries = $calculator->setPositionsWithinSQL($sql, $queries);
        }
        $this->parsed = $queries;
        return $this->parsed;
    }

    public function parse($sql, $calcPositions = false) {
        $processor = new DefaultProcessor();
        $queries = $processor->process($sql);
        if ($calcPositions) {
            $calculator = new PositionCalculator();
            $queries = $calculator->setPositionsWithinSQL($sql, $queries);
        }
        $this->parsed = $queries;
        return $this->EsBuilder();
        //return $this->parsed;
    }

    private function EsBuilder(){
        //table
        if(isset($this->parsed['FROM']) && !empty($this->parsed['FROM'])){
            $this->table($this->parsed['FROM']);
        }
        //insert
        if(isset($this->parsed['INSERT']) && !empty($this->parsed['INSERT'])){
            $this->insert($this->parsed['INSERT']);
        }

        //update
        if(isset($this->parsed['UPDATE']) && !empty($this->parsed['UPDATE'])){
            $this->update($this->parsed['UPDATE']);
        }
        //set
        if(isset($this->parsed['SET']) && !empty($this->parsed['SET'])){
            $this->updateset($this->parsed['SET']);
        }
        //delete
        if(isset($this->parsed['DELETE']) && !empty($this->parsed['DELETE'])){
            $this->delete($this->parsed['DELETE']);
        }
        //limit
        if(isset($this->parsed['LIMIT']) && !empty($this->parsed['LIMIT'])){
            $this->limit($this->parsed['LIMIT']);
            if(isset($this->parsed['GROUP']) && !empty($this->parsed['GROUP'])){
                $this->Builderarr['size']=0;
                $this->limit($this->parsed['LIMIT']);
            }else{
                $this->Builderarr['from']=$this->limit['from'] * $this->limit['size'];
                $this->Builderarr['size']=$this->limit['size'];
            }
        }else{
            $this->limit(array());
        }
        //having
        if(isset($this->parsed['HAVING']) && !empty($this->parsed['HAVING'])){
            $this->having($this->parsed['HAVING']);
        }
        //where
        if(isset($this->parsed['WHERE']) && !empty($this->parsed['WHERE'])){
            $this->where($this->parsed['WHERE']);
        }
        //groupby
        if(isset($this->parsed['GROUP']) && !empty($this->parsed['GROUP'])){
            $this->groupby($this->parsed['GROUP']);
            if(!empty($this->agg['aggs'])){
                $this->Builderarr['aggs']=$this->agg['aggs'];
            }
        }
        //orderby
        if(isset($this->parsed['ORDER']) && !empty($this->parsed['ORDER'])){
            $this->orderby($this->parsed['ORDER']);
            if(!empty($this->sort['sort'])){
                $this->Builderarr['sort']=$this->sort['sort'];
            }
        }
        //select
        if(isset($this->parsed['SELECT']) && !empty($this->parsed['SELECT'])){
            $this->select($this->parsed['SELECT']);
        }
        if(!isset($this->Builderarr) && empty($this->Builderarr)){
            $this->Builderarr['query']['match_all']=(object)array();
        }
        return $this;
    }
    public function build(){
        //request
        return $this->PostEs($this->Builderarr);
    }
    public function explain(){
        $this->explain=json_encode($this->Builderarr,true);
        return $this->explain;
    }


     public function scroll($scrollid=''){
        $this->isscroll=1;
        if($scrollid){
            $this->scrollurl=$this->basescrollurl;
            $this->scrollurl .="/_search/scroll?pretty";
            $this->Builderarr=array();
            $this->Builderarr['scroll']=$this->scrolltime;
            $this->Builderarr['scroll_id']=$scrollid;
        }else{
            $this->scrollurl .="/".$this->index_es."/".$this->type_es."/_search?pretty&scroll=".$this->scrolltime;
        }
        $this->url=$this->scrollurl;
        return $this->PostEs($this->Builderarr);
    }


    private function table($arr){
        if(isset($this->parsed['DELETE']) && !empty($this->parsed['DELETE'])){
            foreach ($arr as $v) {
                if($v['table']){
                    if ($this->version_es == '7.x'){
                        $this->url .="/".$this->index_es."/_delete_by_query?pretty";
                    }else {
                        $this->url .="/".$this->index_es."/".$this->type_es."/_delete_by_query?pretty";
                    }  
                }
            }
        }else{
            foreach ($arr as $v) {
                if($v['table']){
                    if ($this->version_es == '7.x'){
                        $this->url .="/".$this->index_es."/_search?pretty";
                    } else {
                        $this->url .="/".$this->index_es."/".$this->type_es."/_search?pretty";
                    }
                }
            }
        }
        
    }

    private function insert($arr){
        if ($this->version_es == '7.x'){
            $this->url .="/".$this->index_es."?pretty";
        }else {
            $this->url .="/".$this->index_es."/".$this->type_es."?pretty";
        }
        foreach ($arr as $k=>$v) {
            if(count($v['columns'])>0){
                $this->Builderarr=$this->resdata($v['columns'],$this->parsed['VALUES'][$k]['data']);
            }
        }

    }

    private function resdata($data,$value){
        foreach ($data as $v) {
            if($v['base_expr']){
                $fielddata=str_replace('`','',$v['base_expr']);
                $fieldarr[]=$fielddata;
            }
        }
        foreach ($value as $vv) {
            if(strlen($vv['base_expr'])){
                $fielddata=str_replace("'",'',$vv['base_expr']);
                $fielddata=str_replace('"','',$fielddata);
                $valuearr[]=$fielddata;
            }
        }
        return array_combine($fieldarr,$valuearr);
    }

    private function update($arr){
        foreach ($arr as $v) {
            if($v['table']){
                $this->table=$v['table'];
                $this->url .="/".$this->index_es."/".$this->type_es."/";
            }
        }
    }

    private function delete($arr){
    }

    private function getEsData($url){
            $ch = curl_init($url);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1) ;
            curl_setopt($ch, CURLOPT_TIMEOUT, 60);
            curl_setopt($ch, CURLOPT_HEADER, 0);
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
            curl_setopt($ch, CURLOPT_SSLVERSION, 3);
            curl_setopt($ch, CURLOPT_BINARYTRANSFER, true) ;
            $output = curl_exec($ch);
            if($output === false)  //超时处理
                { 
                    if(curl_errno($ch) == CURLE_OPERATION_TIMEDOUT)  
                    {  
                     my_file_put_contents("getEsData.txt", "时间：".date('Ymd-H:i:s',time())."\r\n错误内容为：curl通过get方式请求{$url}的连接超时\r\n");
                    }  
            }
            curl_close($ch);
           $output=json_decode($output,true);
           if (empty($output)) {
              return array();
            }
            return $output['version']['number'];
    }



    private function PostEs($postdata,$json=true,$token=false){
        $url=$this->url;
        $datastring = json_encode($postdata,true);
        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_URL, $url) ;
        curl_setopt($ch, CURLOPT_POST, 1) ;
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLINFO_HEADER_OUT, true);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_TIMEOUT, 60);   //只需要设置一个秒的数量就可以
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
        curl_setopt($ch, CURLOPT_SSLVERSION, 3);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $datastring);
        if ($json) {
              curl_setopt($ch, CURLOPT_HTTPHEADER, array(
                    'Content-Type: application/json;',
                    'Content-Length: ' . strlen($datastring))
                );
        }
        if ($token) {
                curl_setopt( $ch, CURLOPT_HTTPHEADER, array(
                        'Content-Type: application/json; charset=utf-8',
                        'Content-Length: ' . strlen($datastring),
                        'Authorization:'.$token
                    )
                );
        }
        $output=curl_exec($ch);
        if($output === false)  //超时处理
            { 
                if(curl_errno($ch) == CURLE_OPERATION_TIMEDOUT)  
                {  
                 file_put_contents("getEsData.txt", "时间：".date('Ymd-H:i:s',time())."\r\n错误内容为：curl通过post方式请求{$this->url}的连接超时\r\n");
                }  
        }
        curl_close($ch);
        $output=json_decode($output,true);
        if (empty($output)) {
              $this->result=json_encode(array(),true);
        }
        if(isset($output['error'])){
            $this->result=json_encode($output,true);
        }else if(isset($this->parsed['UPDATE']) && !empty($this->parsed['UPDATE'])){
            $update_arr=$output['_shards'];
            unset($update_arr['total']);
            $this->result=json_encode($update_arr,true);
        }else if(isset($this->parsed['DELETE']) && !empty($this->parsed['DELETE'])){
            $delete_arr['total']=$output['total'];
            $delete_arr['deleted']=$output['deleted'];
            $delete_arr['successfull']=$output['deleted'];
            $this->result=json_encode($delete_arr,true);
        }else if(isset($this->parsed['INSERT']) && !empty($this->parsed['INSERT'])){
            $this->result=json_encode($output,true);
        }else{
            if ($this->version_es == '7.x'){
                $total_str=$output['hits']['total']['value'];
            }else {
                $total_str=$output['hits']['total'];
            }
            if(isset($this->parsed['GROUP']) && !empty($this->parsed['GROUP'])){
                if($output['hits']['hits'] && empty($output['aggregations'][$this->fistgroup]['buckets'])){
                    $tmp_counter=count($output['hits']['hits']);
                    $counter=($this->limit['from'] + 1 )*$this->limit['size'];
                    if($tmp_counter<$counter){
                        $page=ceil($tmp_counter/$this->limit['size']);
                        $outputs['page']=$page==0?1:$page;
                        $outputs['result']=array_slice($output['hits']['hits'],($page-1)*$this->limit['size'],$tmp_counter-(($page-1)*$this->limit['size']));
                    }else if($tmp_counter==$counter){
                        $outputs['page']=$this->limit['from']+1;
                        $outputs['result']=array_slice($output['hits']['hits'],-$this->limit['size']);
                    }else{
                        $page=$this->limit['from']+1;
                        $outputs['page']=$page==0?1:$page;
                        $outputs['result']=array_slice($output['hits']['hits'],($page-1)*$this->limit['size'],$this->limit['size']);
                    }
                }else if(isset($output['aggregations'][$this->fistgroup]['buckets']) && !empty($output['aggregations'][$this->fistgroup]['buckets'])){
                    $tmp_counter=count($output['aggregations'][$this->fistgroup]['buckets']);
                    $counter=($this->limit['from'] + 1 )*$this->limit['size'];
                    if($tmp_counter<$counter){
                        $page=ceil($tmp_counter/$this->limit['size']);
                        $outputs['page']=$page==0?1:$page;
                        $outputs['result'][$this->fistgroup]['buckets']=array_slice($output['aggregations'][$this->fistgroup]['buckets'],($page-1)*$this->limit['size'],$tmp_counter-(($page-1)*$this->limit['size']));
                    }else if($tmp_counter==$counter){
                        $outputs['page']=$this->limit['from']+1;
                        $outputs['result'][$this->fistgroup]['buckets']=array_slice($output['aggregations'][$this->fistgroup]['buckets'],-$this->limit['size']);
                    }else{
                        $page=$this->limit['from']+1;
                        $outputs['page']=$page==0?1:$page;
                        $outputs['result'][$this->fistgroup]['buckets']=array_slice($output['aggregations'][$this->fistgroup]['buckets'],($page-1)*$this->limit['size'],$this->limit['size']);
                    }
                }else{
                    $tmp_counter=count($output['aggregations'][$this->fistgroup]['buckets']);
                    $counter=($this->limit['from'] + 1 )*$this->limit['size'];
                    if($tmp_counter<$counter){
                        $page=ceil($tmp_counter/$this->limit['size']);
                        $outputs['page']=$page==0?1:$page;
                        $outputs['result']=array_slice($output['aggregations'][$this->fistgroup]['buckets'],($page-1)*$this->limit['size'],$tmp_counter-(($page-1)*$this->limit['size']));
                    }else if($tmp_counter==$counter){
                        $outputs['page']=$this->limit['from']+1;
                        $outputs['result']=array_slice($output['aggregations'][$this->fistgroup]['buckets'],-$this->limit['size']);
                    }
                }
            }else{
                if(isset($output['aggregations']) && !empty($output['aggregations'])){
                    $outputs['result']=$output['aggregations'];
                }else{
                    $page_tmp=ceil($total_str/$this->limit['size']);
                    $page=$this->limit['from'] + 1 ;
                    if($page_tmp>=$page){
                    }else{
                        $page=$page_tmp;
                        if($page_tmp!=0){
                            $this->Builderarr['from']=($page_tmp-1) * $this->limit['size']+1;
                        }else{
                            $this->Builderarr['from']=0;
                        }
                        $this->Builderarr['size']=$this->limit['size'];
                    }
                    $outputs['page']=$page==0?1:$page;
                    $outputs['result']=$output['hits']['hits'];
                }
            }
            $outputs['total']=$total_str;
            if($this->isscroll && isset($output['_scroll_id'])){
                $outputs['scrollid']=$output['_scroll_id'];
            }
            $this->result=json_encode($outputs,true);
        }
        return $this->result;
        
    }

    private function where($arr){
        for($i=0;$i<count($arr);$i++){
            if($arr[$i]['expr_type']=='bracket_expression'){
                if($arr[$i]['sub_tree']){
                    if(count($arr[$i]['sub_tree'])>1){
                        if(isset($arr[$i]['sub_tree'][0]['expr_type']) && $arr[$i]['sub_tree'][0]['expr_type']=='bracket_expression'){
                            for($jj=0;$jj<count($arr[$i]['sub_tree']);$jj++){
                                $this->whereor($arr[$i]['sub_tree'],$jj);
                            }
                        }else{
                            $tmp_arr=$arr[$i]['sub_tree'];
                            for($j=0;$j<count($tmp_arr);$j++){
                                $this->whereext($tmp_arr,$j);
                            }
                        }
                    }else{
                        if(isset($arr[$i]['sub_tree'][0]['expr_type']) && $arr[$i]['sub_tree'][0]['expr_type']=='bracket_expression'){
                            $tmp_arr=$arr[$i]['sub_tree'][0]['sub_tree'];
                        }else{
                            $tmp_arr=$arr[$i]['sub_tree'];
                        }
                        for($j=0;$j<count($tmp_arr);$j++){
                            $this->whereext($tmp_arr,$j);
                        }
                    }
                }
            }else{
                $this->whereext($arr,$i);
            }
            
        }
    }

    private function whereorext($arr){
        $tmp_or=array();
        for($i=0;$i<count($arr);$i++){
            if(!is_numeric($arr[$i]['base_expr'])){
                $lowerstr = strtolower($arr[$i]['base_expr']);
            }else{
                $lowerstr = $arr[$i]['base_expr'];
            }
            switch ($lowerstr) {
                case '=':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='and' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='and'){
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $tmp_or['bool']['must'][]=$term;
                        }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $tmp_or['bool']['must'][]=$term;
                        }
                    }else{
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                           if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $tmp_or['bool']['must'][]=$term;
                        }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $tmp_or['bool']['must'][]=$term;
                        }
                    }
                break;
            case '!=':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='and' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='and'){
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $tmp_or['bool']['must_not'][]=$term;
                        }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $tmp_or['bool']['must_not'][]=$term;
                        }
                    }else{
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                           if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $tmp_or['bool']['must_not'][]=$term;
                        }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $tmp_or['bool']['must_not'][]=$term;
                        }
                    }
                break;
            case '<>':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='and' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='and'){
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $tmp_or['bool']['must_not'][]=$term;
                        }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $tmp_or['bool']['must_not'][]=$term;
                        }
                    }else{
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                           if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $tmp_or['bool']['must_not'][]=$term;
                        }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $tmp_or['bool']['must_not'][]=$term;
                        }
                    }
                break;
            case 'in':
                if(strtolower($arr[$i-1]['base_expr'])=='not'){
                        break;
                }
                if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                if(isset($arr[$i+1]['sub_tree']) && !empty($arr[$i+1]['sub_tree'])){
                        foreach ($arr[$i+1]['sub_tree'] as &$vv) {
                            if(!is_numeric($vv['base_expr']) && $this->version_es=='8.x'){
                                $termk .='.keyword';
                            }
                            $tmp_da_str=str_replace('"','',$vv['base_expr']);
                            $tmp_da_str=str_replace("'","",$tmp_da_str);
                            $tmp_or['terms'][$termk][]=$tmp_da_str;
                        }
                    }
                break;
            case 'not':
                if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                switch (strtolower($arr[$i+1]['base_expr'])) {
                        case 'in':
                            if(isset($arr[$i+2]['sub_tree']) && !empty($arr[$i+2]['sub_tree'])){
                                foreach ($arr[$i+2]['sub_tree'] as &$vv) {
                                    if(!is_numeric($vv['base_expr']) && $this->version_es=='8.x'){
                                        $termk .='.keyword';
                                    }
                                    $tmp_da_str=str_replace('"','',$vv['base_expr']);
                                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                                    $tmp_or['bool']['must_not']['terms'][$termk][]=$tmp_da_str;
                                }
                            }
                            break;
                        
                        case 'like':
                            $tmp_la_str=str_replace('"','',$arr[$i+2]['base_expr']);
                            $tmp_la_str=str_replace("'","",$tmp_la_str);
                            if(!is_numeric($arr[$i+2]['base_expr']) && $this->version_es=='8.x'){
                                //$term['match_phrase'][$termk.'.keyword']=str_replace("%","",$tmp_la_str);
                                $term['wildcard'][$termk.'.keyword']=str_replace("%","*",$tmp_la_str);
                                $tmp_or['bool']['must_not'][]=$term;
                            }else{
                                //$term['match_phrase'][$termk]=str_replace("%","",$tmp_la_str);
                                $term['wildcard'][$termk]=str_replace("%","*",$tmp_la_str);
                                $tmp_or['bool']['must_not'][]=$term;
                            }
                            break;
                            
                        case 'null':
                            $tmp_or['exists']['field']=$arr[$i-2]['base_expr'];
                            break;
                    }
                break;
            case 'is':
                if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(strtolower($arr[$i+1]['base_expr'])=='not'){
                        break;
                    }
                    $tmp_or['bool']['must_not'][]['exists']['field']=$arr[$i-1]['base_expr'];
                break;
            case '>':
                if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                $tmp_da_str=str_replace("'","",$tmp_da_str);
                $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                $tmp_or['range'][$termk]['gt']=$tmp_da_str;
                if(!isset($tmp_or['range'][$termk]['time_zone']) && $is_date){
                    $tmp_or['range'][$termk]['time_zone']="+08:00";
                }
                break;
            case '>=':
                if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                $tmp_da_str=str_replace("'","",$tmp_da_str);
                $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                $tmp_or['range'][$termk]['gte']=$tmp_da_str;
                if(!isset($tmp_or['range'][$termk]['time_zone']) && $is_date){
                    $tmp_or['range'][$termk]['time_zone']="+08:00";
                }
                break;
            case '<':
                if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                $tmp_da_str=str_replace("'","",$tmp_da_str);
                $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                $tmp_or['range'][$termk]['lt']=$tmp_da_str;
                if(!isset($tmp_or['range'][$termk]['time_zone']) && $is_date){
                    $tmp_or['range'][$termk]['time_zone']="+08:00";
                }
                break;
            case '<=':
                if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                $tmp_da_str=str_replace("'","",$tmp_da_str);
                $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                $tmp_or['range'][$termk]['lte']=$tmp_da_str;
                if(!isset($tmp_or['range'][$termk]['time_zone']) && $is_date){
                    $tmp_or['range'][$termk]['time_zone']="+08:00";
                }
                break;
            case 'like':
                if(strtolower($arr[$i-1]['base_expr'])=='not'){
                        break;
                }
                if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                $tmp_la_str=str_replace('"','',$arr[$i+1]['base_expr']);
                $tmp_la_str=str_replace("'","",$tmp_la_str);
                if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                    //$term['match_phrase'][$termk.'.keyword']=str_replace("%","",$tmp_la_str);
                    $term['wildcard'][$termk.'.keyword']=str_replace("%","*",$tmp_la_str);
                    $tmp_or['bool']['must'][]=$term;
                }else{
                    //$term['match_phrase'][$termk]=str_replace("%","",$tmp_la_str);
                    $term['wildcard'][$termk]=str_replace("%","*",$tmp_la_str);
                    $tmp_or['bool']['must'][]=$term;
                }
                break;
            case 'between':
                if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                 $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                 $tmp_da_str=str_replace("'","",$tmp_da_str);
                 $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                 $tmp_or['range'][$termk]['gte']=$tmp_da_str;
                 if(!isset($tmp_or['range'][$termk]['time_zone']) && $is_date){
                    $tmp_or['range'][$termk]['time_zone']="+08:00";
                 }
                 $tmp_da_str=str_replace('"','',$arr[$i+3]['base_expr']);
                 $tmp_da_str=str_replace("'","",$tmp_da_str);
                 $tmp_or['range'][$termk]['lte']=$tmp_da_str;
                break;
            
            }     
        }
        return $tmp_or;
    }

    private function whereorink($arr,$i){
        $tmparrs=$arr;
        if(isset($tmparrs[$i]['base_expr']) && strtolower($tmparrs[$i]['base_expr'])!='or'){
            $this->arrtmp[]=$arr[$i];
            $i=$i+1;
            $this->whereorink($tmparrs,$i);
        }
        return $this->arrtmp;
    }

    private function whereor($arr,$i){
        if(!is_numeric($arr[$i]['base_expr'])){
                $lowerstr = strtolower($arr[$i]['base_expr']);
            }else{
                $lowerstr = $arr[$i]['base_expr'];
            }
            switch ($lowerstr) {
                case 'or':
                    if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                            if($this->tmp_str_filter=='' && !$this->tmp_or){
                                $this->count_tmp_filter++;
                            }
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                    if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_str!='' && $this->tmp_lock_str!=$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }
                        }
                    if(!isset($arr[$i-2])){
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]['bool']['should'][]=$this->whereorext($arr[$i-1]['sub_tree']);
                    }
                    if($arr[$i+1]['expr_type']=='bracket_expression'){
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]['bool']['should'][]=$this->whereorext($arr[$i+1]['sub_tree']);
                    }else{
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]['bool']['should'][]=$this->whereorext($this->whereorink($arr,$i+1));
                        $this->arrtmp=array();
                    }
                    $this->tmp_or=1;
                  break;
                case 'and':
                    if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                            if($this->tmp_str_filter=='' && !$this->tmp_and){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                    if(!isset($arr[$i-2])){
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][]=$this->whereorext($arr[$i-1]['sub_tree']);
                    }
                    if($arr[$i+1]['expr_type']=='bracket_expression'){
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][]=$this->whereorext($arr[$i+1]['sub_tree']);
                    }else{
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][]=$this->whereorext($this->whereorink($arr,$i+1));
                        $this->arrtmp=array();
                    }
                    $this->tmp_and=1;
                    break;
            }


    }


    private function whereext($arr,$i){
        if(!is_numeric($arr[$i]['base_expr'])){
                $lowerstr = strtolower($arr[$i]['base_expr']);
            }else{
                $lowerstr = $arr[$i]['base_expr'];
            }
            switch ($lowerstr) {
                case '=':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);

                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_str!='' && $this->tmp_lock_str!=$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp++;
                            }
                        }
                        if(isset($this->parsed['UPDATE']) && !empty($this->parsed['UPDATE'])){
                            $this->url .=$tmp_da_str ."/_update?pretty";
                        }else{
                            if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_tmp]['bool']['should'][]=$term;
                            }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_tmp]['bool']['should'][]=$term;
                            }
                                unset($term['match_phrase']);
                        }
                        $this->tmp_lock=$lowerstr;
                        $this->tmp_lock_str=$lowerstr;
                    }else if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='and' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='and'){
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock_str!='' && $this->tmp_lock_str!=$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp++;
                            }
                        }
                        if(isset($this->parsed['UPDATE']) && !empty($this->parsed['UPDATE'])){
                            $this->url .=$tmp_da_str ."/_update?pretty";
                        }else{
                            if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                                if($this->tmp_str_filter==''){
                                    $this->count_tmp_filter++;
                                }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                    $this->count_tmp_filter++;
                                }
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                            if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][]=$term;
                            }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][]=$term;
                            }
                                unset($term['match_phrase']);
                        }
                        $this->tmp_lock_str=$lowerstr;
                    }else{
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                           if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                                if($this->tmp_str_filter==''){
                                    $this->count_tmp_filter++;
                                }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                    $this->count_tmp_filter++;
                                }
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                            if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][]=$term;
                            }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][]=$term;
                            }
                                unset($term['match_phrase']);
                            $this->tmp_lock_str=$lowerstr;
                    }
                    $this->tmp_lock=$lowerstr;
                    $this->tmp_str=$lowerstr;
                    break;
                case '!=':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);

                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][0]) && $this->tmp_lock_str!='' && $this->tmp_lock_str!=$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp++;
                            }
                        }
                        if(isset($this->parsed['UPDATE']) && !empty($this->parsed['UPDATE'])){
                            $this->url .=$tmp_da_str ."/_update?pretty";
                        }else{
                            if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][$this->count_tmp]['bool']['should'][]=$term;
                            }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][$this->count_tmp]['bool']['should'][]=$term;
                            }
                                unset($term['match_phrase']);
                        }
                        $this->tmp_lock=$lowerstr;
                        $this->tmp_lock_str=$lowerstr;
                    }else if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='and' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='and'){
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock_str!='' && $this->tmp_lock_str!=$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp++;
                            }
                        }
                        if(isset($this->parsed['UPDATE']) && !empty($this->parsed['UPDATE'])){
                            $this->url .=$tmp_da_str ."/_update?pretty";
                        }else{
                            if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                                if($this->tmp_str_filter==''){
                                    $this->count_tmp_filter++;
                                }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                    $this->count_tmp_filter++;
                                }
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                            if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][]=$term;
                            }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][]=$term;
                            }
                                unset($term['match_phrase']);
                        }
                        $this->tmp_lock_str=$lowerstr;
                    }else{
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                           if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                                if($this->tmp_str_filter==''){
                                    $this->count_tmp_filter++;
                                }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                    $this->count_tmp_filter++;
                                }
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                            if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][]=$term;
                            }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][]=$term;
                            }
                                unset($term['match_phrase']);
                            $this->tmp_lock_str=$lowerstr;
                    }
                    $this->tmp_lock=$lowerstr;
                    $this->tmp_str=$lowerstr;
                    break;
                case '<>':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);

                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][0]) && $this->tmp_lock_str!='' && $this->tmp_lock_str!=$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp++;
                            }
                        }
                        if(isset($this->parsed['UPDATE']) && !empty($this->parsed['UPDATE'])){
                            $this->url .=$tmp_da_str ."/_update?pretty";
                        }else{
                            if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][$this->count_tmp]['bool']['should'][]=$term;
                            }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][$this->count_tmp]['bool']['should'][]=$term;
                            }
                                unset($term['match_phrase']);
                        }
                        $this->tmp_lock=$lowerstr;
                        $this->tmp_lock_str=$lowerstr;
                    }else if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='and' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='and'){
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock_str!='' && $this->tmp_lock_str!=$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp++;
                            }
                        }
                        if(isset($this->parsed['UPDATE']) && !empty($this->parsed['UPDATE'])){
                            $this->url .=$tmp_da_str ."/_update?pretty";
                        }else{
                            if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                                if($this->tmp_str_filter==''){
                                    $this->count_tmp_filter++;
                                }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                    $this->count_tmp_filter++;
                                }
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                            if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][]=$term;
                            }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][]=$term;
                            }
                                unset($term['match_phrase']);
                        }
                        $this->tmp_lock_str=$lowerstr;
                    }else{
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                           if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                                if($this->tmp_str_filter==''){
                                    $this->count_tmp_filter++;
                                }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                    $this->count_tmp_filter++;
                                }
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                            if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][]=$term;
                            }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][]=$term;
                            }
                                unset($term['match_phrase']);
                            $this->tmp_lock_str=$lowerstr;
                    }
                    $this->tmp_lock=$lowerstr;
                    $this->tmp_str=$lowerstr;
                    break;
                case 'in':
                    if(strtolower($arr[$i-1]['base_expr'])=='not'){
                        break;
                    }
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock==$lowerstr){
                        if($this->tmp_str_filter==''){
                            $this->count_tmp_filter++;
                        }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                            $this->count_tmp_filter++;
                        }
                    }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                            $this->count_tmp_filter++;
                    }
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(isset($this->Builderarr['query']['bool']['filter']['bool']['should'][$this->count_tmp]) && $this->tmp_lock_str!='' && $this->tmp_lock_str==$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp++;
                            }
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                            $this->count_tmp++;
                        }
                        if(isset($arr[$i+1]['sub_tree']) && !empty($arr[$i+1]['sub_tree'])){
                            foreach ($arr[$i+1]['sub_tree'] as &$vv) {
                                if(!is_numeric($vv['base_expr']) && $this->version_es=='8.x'){
                                    $termk .='.keyword';
                                }
                                $tmp_da_str=str_replace('"','',$vv['base_expr']);
                                $tmp_da_str=str_replace("'","",$tmp_da_str);
                             $this->Builderarr['query']['bool']['filter']['bool']['should'][$this->count_tmp]['terms'][$termk][]=$tmp_da_str;
                        }
                    }
                }else{
                    if(isset($arr[$i+1]['sub_tree']) && !empty($arr[$i+1]['sub_tree'])){
                        if ($this->version_es == '7.x'){
                            $this->count_tmp_filter++;
                        }
                        foreach ($arr[$i+1]['sub_tree'] as &$vv) {
                            if(!is_numeric($vv['base_expr']) && $this->version_es=='8.x'){
                                $termk .='.keyword';
                            }
                            $tmp_da_str=str_replace('"','',$vv['base_expr']);
                            $tmp_da_str=str_replace("'","",$tmp_da_str);
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['terms'][$termk][]=$tmp_da_str;
                        }
                    }
                }
                    $this->tmp_lock=$lowerstr;
                    $this->tmp_str=$termk;
                    unset($termk);
                    break;
                case 'not':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock==$lowerstr){
                        if($this->tmp_str_filter==''){
                            $this->count_tmp_filter++;
                        }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                            $this->count_tmp_filter++;
                        }
                    }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                            $this->count_tmp_filter++;
                    }
                    switch (strtolower($arr[$i+1]['base_expr'])) {
                        case 'in':
                            if(isset($arr[$i+2]['sub_tree']) && !empty($arr[$i+2]['sub_tree'])){
                                foreach ($arr[$i+2]['sub_tree'] as &$vv) {
                                    if(!is_numeric($vv['base_expr']) && $this->version_es=='8.x'){
                                        $termk .='.keyword';
                                    }
                                    $tmp_da_str=str_replace('"','',$vv['base_expr']);
                                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                                    $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not']['terms'][$termk][]=$tmp_da_str;
                                }
                            }
                            break;
                        
                        case 'like':
                            $tmp_la_str=str_replace('"','',$arr[$i+2]['base_expr']);
                            $tmp_la_str=str_replace("'","",$tmp_la_str);
                            if(!is_numeric($arr[$i+2]['base_expr']) && $this->version_es=='8.x'){
                                // $term['match_phrase'][$termk.'.keyword']=str_replace("%","",$tmp_la_str);
                                $term['wildcard'][$termk.'.keyword']=str_replace("%","*",$tmp_la_str);
                                $this->Builderarr['query']['filter'][$this->count_tmp_filter]['bool']['must_not'][]=$term;
                            }else{
                                //$term['match_phrase'][$termk]=str_replace("%","",$tmp_la_str);
                                $term['wildcard'][$termk]=str_replace("%","*",$tmp_la_str);
                                $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][]=$term;
                            }
                            break;
                        case 'null':
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['exists']['field']=$arr[$i-2]['base_expr'];
                            break;
                    }
                    $this->tmp_lock=$lowerstr;
                    $this->tmp_str=$termk;
                    unset($termk);
                    break;
                case 'is':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(strtolower($arr[$i+1]['base_expr'])=='not'){
                        break;
                    }
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                            if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock==$lowerstr){
                        if($this->tmp_str_filter==''){
                            $this->count_tmp_filter++;
                        }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                            $this->count_tmp_filter++;
                        }
                    }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                            $this->count_tmp_filter++;
                    }
                    $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][]['exists']['field']=$arr[$i-1]['base_expr'];
                    $this->tmp_lock=$lowerstr;
                    $this->tmp_str=$termk;
                    unset($termk);
                    break;
                case '>':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(isset($this->Builderarr['query']['bool']['filter'][0]) && $this->tmp_lock!='' && $this->tmp_lock==$lowerstr){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_fi!='' && $this->tmp_lock_fi==$lowerstr){
                            if($this->tmp_fi==''){
                                $this->count_fi++;
                            }else if($this->tmp_fi!='' && $this->tmp_fi!=$termk){
                                $this->count_fi++;
                            }
                        }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][0]) && $this->tmp_lock_range!=''){
                            if($this->tmp_str_range==''){
                                $this->count_tmp_range++;
                            }else if($this->tmp_str_range!='' && $this->tmp_str_range!=$termk){
                                $this->count_tmp_range++;
                            }
                        }
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['gt']=$tmp_da_str;
                         if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']) && $is_date){
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']="+08:00";
                        }
                    }else{
                        if(isset($this->Builderarr['query']['bool']['filter'][0]) && $this->tmp_lock_str!='' && $this->tmp_lock_str!=$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp++;
                            }
                        }
                        if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range']) && $this->tmp_lock!='' ){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['gt']=$tmp_da_str;
                        if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']) && $is_date){
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']="+08:00";
                        }
                    }
                    $this->tmp_str=$termk;
                    $this->tmp_lock_str=$lowerstr;
                    $this->tmp_lock=$lowerstr;
                    $this->tmp_lock_range=$lowerstr;
                    $this->tmp_lock_fi=$lowerstr;
                    break;
                case '>=':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(isset($this->Builderarr['query']['bool']['filter'][0]) && $this->tmp_lock!='' && $this->tmp_lock==$lowerstr){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_fi!='' && $this->tmp_lock_fi==$lowerstr){
                            if($this->tmp_fi==''){
                                $this->count_fi++;
                            }else if($this->tmp_fi!='' && $this->tmp_fi!=$termk){
                                $this->count_fi++;
                            }
                        }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][0]) && $this->tmp_lock_range!='' ){
                            if($this->tmp_str_range==''){
                                $this->count_tmp_range++;
                            }else if($this->tmp_str_range!='' && $this->tmp_str_range!=$termk){
                                $this->count_tmp_range++;
                            }
                        }
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['gte']=$tmp_da_str;
                         if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']) && $is_date){
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']="+08:00";
                        }
                    }else{
                        if(isset($this->Builderarr['query']['bool']['filter'][0]) && $this->tmp_lock_str!='' && $this->tmp_lock_str!=$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp++;
                            }
                        }
                        if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range']) && $this->tmp_lock!='' ){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['gte']=$tmp_da_str;
                        if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']) && $is_date){
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']="+08:00";
                        }
                    }
                    $this->tmp_str=$termk;
                    $this->tmp_lock_str=$lowerstr;
                    $this->tmp_lock=$lowerstr;
                    $this->tmp_lock_range=$lowerstr;
                    $this->tmp_lock_fi=$lowerstr;
                    break;
                case '<':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(isset($this->Builderarr['query']['bool']['filter'][0]) && $this->tmp_lock!='' && $this->tmp_lock==$lowerstr){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }
                         if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_fi!='' && $this->tmp_lock_fi==$lowerstr){
                            if($this->tmp_fi==''){
                                $this->count_fi++;
                            }else if($this->tmp_fi!='' && $this->tmp_fi!=$termk){
                                $this->count_fi++;
                            }
                        }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][0]) && $this->tmp_lock_range!=''){
                            if($this->tmp_str_range==''){
                                $this->count_tmp_range++;
                            }else if($this->tmp_str_range!='' && $this->tmp_str_range!=$termk){
                                $this->count_tmp_range++;
                            }
                        }
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['lt']=$tmp_da_str;
                         if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']) && $is_date){
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']="+08:00";
                        }
                    }else{
                        if(isset($this->Builderarr['query']['bool']['filter'][0]) && $this->tmp_lock_str!='' && $this->tmp_lock_str==$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }
                        }
                        if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range']) && $this->tmp_lock!='' ){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['lt']=$tmp_da_str;
                        if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']) && $is_date){
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']="+08:00";
                        }
                    }
                    
                    $this->tmp_str=$termk;
                    $this->tmp_lock_str=$lowerstr;
                    $this->tmp_lock=$lowerstr;
                    $this->tmp_lock_range=$lowerstr;
                    $this->tmp_lock_fi=$lowerstr;
                    break;
                case '<=':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(isset($this->Builderarr['query']['bool']['filter'][0]) && $this->tmp_lock!='' && $this->tmp_lock==$lowerstr){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_fi!='' && $this->tmp_lock_fi==$lowerstr){
                            if($this->tmp_fi==''){
                                $this->count_fi++;
                            }else if($this->tmp_fi!='' && $this->tmp_fi!=$termk){
                                $this->count_fi++;
                            }
                        }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][0]) && $this->tmp_lock_range!='' ){
                            if($this->tmp_str_range==''){
                                $this->count_tmp_range++;
                            }else if($this->tmp_str_range!='' && $this->tmp_str_range!=$termk){
                                $this->count_tmp_range++;
                            }
                        }
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['lte']=$tmp_da_str;
                         if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']) && $is_date){
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']="+08:00";
                        }
                    }else{
                        if(isset($this->Builderarr['query']['bool']['filter'][0]) && $this->tmp_lock_str!='' && $this->tmp_lock_str==$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp++;
                            }
                        }
                        if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range']) && $this->tmp_lock!=''){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp_filter++;
                            }
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['lte']=$tmp_da_str;
                        if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']) && $is_date){
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']="+08:00";
                        }
                    }
                    $this->tmp_str=$termk;
                    $this->tmp_lock_str=$lowerstr;
                    $this->tmp_lock=$lowerstr;
                    $this->tmp_lock_range=$lowerstr;
                    $this->tmp_lock_fi=$lowerstr;
                    break;
                case 'like':
                    if(strtolower($arr[$i-1]['base_expr'])=='not'){
                        break;
                    }
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    $tmp_la_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_la_str=str_replace("'","",$tmp_la_str);
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(isset($this->Builderarr['query']['bool']['filter'][0]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                            if($this->tmp_str_filter==''){
                                $this->count_tmp_filter++;
                            }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                $this->count_tmp_filter++;
                            }
                        }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_fi!='' && $this->tmp_lock_fi!=$lowerstr){
                            if($this->tmp_fi==''){
                                $this->count_fi++;
                            }else if($this->tmp_fi!='' && $this->tmp_fi!=$termk){
                                $this->count_fi++;
                            }
                        }
                         if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                            //$term['match_phrase'][$termk.'.keyword']=str_replace("%","",$tmp_la_str);
                            $term['wildcard'][$termk.'.keyword']=str_replace("%","*",$tmp_la_str);
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][]=$term;
                        }else{
                            //$term['match_phrase'][$termk]=str_replace("%","",$tmp_la_str);
                            $term['wildcard'][$termk]=str_replace("%","*",$tmp_la_str);
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][]=$term;
                        }
                    }else{
                        if(isset($this->Builderarr['query']['bool']['filter'][0]) && $this->tmp_lock_str!='' && $this->tmp_lock_str!=$lowerstr){
                            if($this->tmp_str==''){
                                $this->count_tmp++;
                            }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                                $this->count_tmp++;
                            }
                        }
                        if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                                if($this->tmp_str_filter==''){
                                    $this->count_tmp_filter++;
                                }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                                    $this->count_tmp_filter++;
                                }
                            }
                        if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                            //$term['match_phrase'][$termk.'.keyword']=str_replace("%","",$tmp_la_str);
                            $term['wildcard'][$termk.'.keyword']=str_replace("%","*",$tmp_la_str);
                            $this->Builderarr['query']['filter'][$this->count_tmp_filter]['must'][$this->count_tmp]['bool']['must'][]=$term;
                        }else{
                            //$term['match_phrase'][$termk]=str_replace("%","",$tmp_la_str);
                            $term['wildcard'][$termk]=str_replace("%","*",$tmp_la_str);
                            $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][]=$term;
                        }
                    }
                    unset($term['wildcard']);
                    $this->tmp_lock_str=$lowerstr;
                    $this->tmp_lock=$lowerstr;
                    $this->tmp_lock_fi=$lowerstr;
                    break;
                case 'between':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                     if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        if($term_tmp_arr[1]!='keyword'){
                                $termk=$term_tmp_arr[1];
                            }else{
                                $termk=$arr[$i-1]['base_expr'];
                            } 
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    if(isset($this->Builderarr['query']['bool']['filter'][0]) && $this->tmp_lock_str!='' && $this->tmp_lock_str!=$lowerstr){
                        if($this->tmp_str==''){
                            $this->count_tmp++;
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                            $this->count_tmp++;
                        }
                    }
                    if(isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock!='' && $this->tmp_lock!=$lowerstr){
                        if($this->tmp_str_filter==''){
                            $this->count_tmp_filter++;
                        }else if($this->tmp_str_filter!='' && $this->tmp_str_filter!=$termk){
                            $this->count_tmp_filter++;
                        }
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                    $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['gte']=$tmp_da_str;
                    if(!isset($this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']) && $is_date){
                        $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']="+08:00";
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+3]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $this->Builderarr['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['lte']=$tmp_da_str;
                    $this->tmp_str=$termk;
                    $this->tmp_lock_str=$lowerstr;
                    $this->tmp_lock=$lowerstr;
                    break;
            }
    }


    private function listtree($arr,$aggs,$order){
        $countmp=0;
        for($i=count($arr)-1;$i>=0;$i--){
            if(isset($arr[$i-1])){
                $key_arr=array_keys($arr[$i]);
                if($countmp==0){
                    if(!isset($arr[$i][$key_arr[0]]['date_histogram'])){
                        $arr[$i][$key_arr[0]]['terms']['size']=($this->limit['from'] + 1 )*$this->limit['size'];
                        if($order){
                            $arr[$i][$key_arr[0]]['terms']['order']=$order['order'];
                        }
                    }
                    if(isset($aggs['aggs'])){
                        if(isset($this->havingagg['having']) && !empty($this->havingagg['having'])){
                            $aggs['aggs']['having']=$this->havingagg['having'];
                        }
                        $arr[$i][$key_arr[0]]['aggs']=$aggs['aggs'];
                    }
                    $arr[$i][$key_arr[0]]['aggs']['top']['top_hits']['size']=$this->top_hits;
                    $countmp=1;
                }
                $key_pre=array_keys($arr[$i-1]);
                $arr[$i-1][$key_pre[0]]['aggs']=$arr[$i];
                unset($arr[$i]);
            }else{
                if(count($arr)==1 && $countmp==0){
                    $key_arrs=array_keys($arr[$i]);
                    if(!isset($arr[$i][$key_arrs[0]]['date_histogram'])){
                        $arr[$i][$key_arrs[0]]['terms']['size']=($this->limit['from'] + 1 )*$this->limit['size'];
                        if($order){
                            $arr[$i][$key_arrs[0]]['terms']['order']=$order['order'];
                        }
                    }
                    if(isset($aggs['aggs'])){
                        if(isset($this->havingagg['having']) && !empty($this->havingagg['having'])){
                            $aggs['aggs']['having']=$this->havingagg['having'];
                        }
                        $arr[$i][$key_arrs[0]]['aggs']=$aggs['aggs'];
                    }
                    $arr[$i][$key_arrs[0]]['aggs']['top']['top_hits']['size']=$this->top_hits;
                    $countmp=1;
                }
            }
        }
        return $arr;
    }



    private function groupby($arr){
        $aggs= array();
        $agg= array();
        $agg_orderby=array();
        for($i=0; $i <count($arr); $i++) {
            if(strrpos($arr[$i]['base_expr'],".")){
                $term_tmp_arr=explode(".",$arr[$i]['base_expr']);
                if($term_tmp_arr[1]!='keyword'){
                    $termk=$term_tmp_arr[1];
                    $termk_tmp=$termk;
                }else{
                    $termk=$arr[$i]['base_expr'];
                    $termk_tmp=$term_tmp_arr[0];
                }
            }else{
                $termk=$arr[$i]['base_expr'];
                $termk_tmp=$termk;
            }
            if(isset($this->fistgroup) && $this->fistgroup==''){
                $this->fistgroup=$termk_tmp;
            }
            $agg[$i][$termk_tmp]['terms']['field']=$termk;
            $agg[$i][$termk_tmp]['terms']['size']=($this->limit['from'] + 1 )*$this->limit['size'];
        }
            if(isset($this->parsed['SELECT']) && !empty($this->parsed['SELECT'])){
                foreach ($this->parsed['SELECT'] as $v) {
                    $this->top_hits=1;
                    if(strrpos($v['base_expr'],"*")){
                        //$this->top_hits=1;
                    }else{
                        if($v['expr_type']=='aggregate_function' || $v['expr_type']=='function'){
                            $lowerstr = strtolower($v['base_expr']);
                            switch ($lowerstr) {
                                case 'count':
                                    if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                                        $term_tmp_arrs=explode(".",$v['sub_tree'][0]['base_expr']);
                                         if($term_tmp_arrs[1]!='keyword'){
                                            $cardinalitys[$v['alias']['name']]['cardinality']['field']=$term_tmp_arrs[1];
                                         }else{
                                            $cardinalitys[$v['alias']['name']]['cardinality']['field']=$v['sub_tree'][0]['base_expr'];
                                        }
                                    }else{
                                        $cardinalitys[$v['alias']['name']]['cardinality']['field']=$v['sub_tree'][0]['base_expr'];
                                    }
                                    $tmmp=1;
                                    $agggs['aggs']=$cardinalitys;
                                    $aggs=array_merge_recursive($aggs, $agggs);
                                    unset($cardinalitys);
                                    break;
                                case 'sum':
                                    if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                                        $term_tmp_arrs=explode(".",$v['sub_tree'][0]['base_expr']);
                                        if (!isset($v['alias']['name'])) {
                                            $v['alias']['name']='sum'.$term_tmp_arrs[1];
                                        }
                                        $cardinalitys[$v['alias']['name']]['sum']['field']=$term_tmp_arrs[1];
                                    }else{
                                        if (!isset($v['alias']['name'])) {
                                            $v['alias']['name']='sum'.$v['sub_tree'][0]['base_expr'];
                                        }
                                        $cardinalitys[$v['alias']['name']]['sum']['field']=$v['sub_tree'][0]['base_expr'];
                                    }
                                    $tmmp=1;
                                    $agggs['aggs']=$cardinalitys;
                                    $aggs=array_merge_recursive($aggs, $agggs);
                                    unset($cardinalitys);
                                    break;
                                case 'min':
                                    if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                                        $term_tmp_arrs=explode(".",$v['sub_tree'][0]['base_expr']);
                                        $cardinalitys[$v['alias']['name']]['min']['field']=$term_tmp_arrs[1];
                                    }else{
                                        $cardinalitys[$v['alias']['name']]['min']['field']=$v['sub_tree'][0]['base_expr'];
                                    }
                                    $tmmp=1;
                                    $agggs['aggs']=$cardinalitys;
                                    $aggs=array_merge_recursive($aggs, $agggs);
                                    unset($cardinalitys);
                                    break;
                                case 'max':
                                    if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                                        $term_tmp_arrs=explode(".",$v['sub_tree'][0]['base_expr']);
                                        $cardinalitys[$v['alias']['name']]['max']['field']=$term_tmp_arrs[1];
                                    }else{
                                        $cardinalitys[$v['alias']['name']]['max']['field']=$v['sub_tree'][0]['base_expr'];
                                    }
                                    $tmmp=1;
                                    $agggs['aggs']=$cardinalitys;
                                    $aggs=array_merge_recursive($aggs, $agggs);
                                    unset($cardinalitys);
                                    break;
                                case 'avg':
                                    if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                                        $term_tmp_arrs=explode(".",$v['sub_tree'][0]['base_expr']);
                                        $cardinalitys[$v['alias']['name']]['avg']['field']=$term_tmp_arrs[1];
                                    }else{
                                        $cardinalitys[$v['alias']['name']]['avg']['field']=$v['sub_tree'][0]['base_expr'];
                                    }
                                    $tmmp=1;
                                    $agggs['aggs']=$cardinalitys;
                                    $aggs=array_merge_recursive($aggs, $agggs);
                                    unset($cardinalitys);
                                    break;
                                case 'concat_ws':
                                    $tmp_script='';
                                    $tmp_ps='';
                                    if(isset($v['alias']) && !empty($v['alias'])){
                                        foreach ($agg as $kk => $ve) {
                                            $key_arr=array_keys($ve);
                                            if(isset($ve[$key_arr[0]]['terms']['field']) && $v['alias']['name']==$ve[$key_arr[0]]['terms']['field']){
                                                foreach ($v['sub_tree'] as $ke => $va) {
                                                    if($va['expr_type']=='const'){
                                                        $tmp_ps=str_replace('"','',$va['base_expr']);
                                                        $tmp_ps=str_replace("'","",$tmp_ps);
                                                    }
                                                    if($va['expr_type']=='colref'){
                                                        $tmp_script .="'".$tmp_ps."' + doc['".$va['base_expr']."'].value + ";
                                                    }
                                                }
                                                $tmp_script=substr($tmp_script,6,strlen($tmp_script)-8);
                                                $agg[$kk][$key_arr[0]]['terms']['script']['source']=$tmp_script;
                                                $agg[$kk][$key_arr[0]]['terms']['script']['lang']='painless';
                                                unset($agg[$kk][$key_arr[0]]['terms']['field']);
                                            }
                                        }
                                    }
                                    break;
                                case 'date_format':
                                    $tmp_script='';
                                    $tmp_ps='';
                                    if(isset($v['alias']) && !empty($v['alias'])){
                                        foreach ($agg as $kk => $ve) {
                                            $key_arr=array_keys($ve);
                                            if(isset($ve[$key_arr[0]]['terms']['field']) && $v['alias']['name']==$ve[$key_arr[0]]['terms']['field']){
                                                for ($jj=0;$jj<=count($v['sub_tree'])-1;$jj++) {
                                                    if($v['sub_tree'][$jj]['expr_type']=='const'){
                                                        $tmp_ps=str_replace('"','',$v['sub_tree'][$jj]['base_expr']);
                                                        $tmp_ps=str_replace("'","",$tmp_ps);
                                                        $tmp_ps=str_replace("%","",$tmp_ps);
                                                        $tmp_ps=str_replace("/","",$tmp_ps);
                                                        $tmp_ps=str_replace("-","",$tmp_ps);
                                                        switch ($tmp_ps) {
                                                            case 'Ymd':
                                                                $agg[$kk][$key_arr[0]]['date_histogram']['interval']="day";
                                                                break;
                                                            case 'Ym':
                                                                $agg[$kk][$key_arr[0]]['date_histogram']['interval']="month";
                                                                $agg[$kk][$key_arr[0]]['date_histogram']['format']="yyyy-MM";
                                                                break;
                                                            case 'Y':
                                                                $agg[$kk][$key_arr[0]]['date_histogram']['interval']="year";
                                                                $agg[$kk][$key_arr[0]]['date_histogram']['format']="yyyy";
                                                                break;
                                                            case 'Yu':
                                                                $agg[$kk][$key_arr[0]]['date_histogram']['interval']="week";
                                                                break;
                                                            case 'H':
                                                                $agg[$kk][$key_arr[0]]['date_histogram']['interval']="hour";
                                                                break;
                                                            case 'i':
                                                                $agg[$kk][$key_arr[0]]['date_histogram']['interval']="minute";
                                                                break;
                                                        }
                                                    }
                                                    if($v['sub_tree'][$jj]['expr_type']=='colref'){
                                                        $agg[$kk][$key_arr[0]]['date_histogram']['field']=$v['sub_tree'][$jj]['base_expr'];
                                                        $agg[$kk][$key_arr[0]]['date_histogram']['format']="yyyy-MM-dd";
                                                        $agg[$kk][$key_arr[0]]['date_histogram']['time_zone']="+08:00";
                                                        unset($agg[$kk][$key_arr[0]]['terms']);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    break;    
                            }
                            if(isset($this->parsed['ORDER']) && !empty($this->parsed['ORDER'])){
                                foreach ($this->parsed['ORDER'] as $vv) {
                                    if($vv['base_expr']==$v['alias']['name']){
                                        $agg_orderby['order'][$vv['base_expr']]=$vv['direction'];
                                    }
                                }
                            }
                        }
                    }
            }
        }
         $tmp_tree=$this->listtree($agg,$aggs,$agg_orderby);
         $this->agg['aggs']=$tmp_tree[0];
    }

    private function orderby($arr){
        if(isset($this->parsed['SELECT']) && !empty($this->parsed['SELECT'])){
            foreach ($this->parsed['SELECT'] as $v) {
                    foreach ($arr as $kk=>$vv) {
                        if($v['alias']){
                            if($v['alias']['name']==$vv['base_expr']){
                                unset($arr[$kk]);
                            }
                        }
                    }
            }

        }
        foreach ($arr as &$va) {
            if(strrpos($va['base_expr'],".")){
                $term_tmp_arr=explode(".",$va['base_expr']);
                if($term_tmp_arr[1]!='keyword'){
                    $termk=$term_tmp_arr[1];
                }else{
                    $termk=$va['base_expr'];
                }
            }else{
                $termk=$va['base_expr'];
            }
            $this->sort['sort'][][$termk]['order']=$va['direction'];
        }
    }

    private function limit($arr){
         if(!isset($arr['offset'])){
            $this->limit['from']=0;
        }else{
            $this->limit['from']=$arr['offset'];
        }
        if(!isset($arr['rowcount'])){
            $this->limit['size']=10;
        }else{
            $this->limit['size']=$arr['rowcount'];
        }
    }

    private function haveext($arr,$i){
          if(!is_numeric($arr[$i]['base_expr'])){
                $lowerstr = strtolower($arr[$i]['base_expr']);
            }else{
                $lowerstr = $arr[$i]['base_expr'];
            }
            switch ($lowerstr) {
                case '=':
                    if($arr[$i-1]['base_expr']==$arr[$i+1]['base_expr']){
                        break;
                    }
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                            $termk=$term_tmp_arr[1];
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);

                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_have!='' && $this->tmp_lock_have!=$lowerstr){
                            if($this->tmp_str_filter_have==''){
                                $this->count_tmp_filter_have++;
                            }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        if(isset($this->havingagg['having']['filter']['bool']['must'][0]) && $this->tmp_lock_str_have!='' && $this->tmp_lock_str_have!=$lowerstr){
                            if($this->tmp_str_have==''){
                                $this->count_tmp_have++;
                            }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_have++;
                            }
                        }
                        if(isset($this->parsed['UPDATE']) && !empty($this->parsed['UPDATE'])){
                            $this->url .=$tmp_da_str ."/_update?pretty";
                        }else{
                            if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $this->havingagg['having']['filter']['bool']['must'][$this->count_tmp_have]['bool']['should'][]=$term;
                            }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $this->havingagg['having']['filter']['bool']['must'][$this->count_tmp_have]['bool']['should'][]=$term;
                            }
                                unset($term['match_phrase']);
                        }
                        $this->tmp_lock=$lowerstr;
                        $this->tmp_lock_str=$lowerstr;
                    }else if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='and' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='and'){
                        if(strrpos($arr[$i-1]['base_expr'],".")){
                            $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                            $termk=$term_tmp_arr[1];
                        }else{
                            $termk=$arr[$i-1]['base_expr'];
                        }
                        $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                        $tmp_da_str=str_replace("'","",$tmp_da_str);
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_str_have!='' && $this->tmp_lock_str_have!=$lowerstr){
                            if($this->tmp_str_have==''){
                                $this->count_tmp_have++;
                            }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_have++;
                            }
                        }
                        if(isset($this->parsed['UPDATE']) && !empty($this->parsed['UPDATE'])){
                            $this->url .=$tmp_da_str ."/_update?pretty";
                        }else{
                            if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_have!='' && $this->tmp_lock_have!=$lowerstr){
                                if($this->tmp_str_filter_have==''){
                                    $this->count_tmp_filter_have++;
                                }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                    $this->count_tmp_filter_have++;
                                }
                            }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                            if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                                $term['match_phrase'][$termk.'.keyword']['query']=$tmp_da_str;
                                $this->havingagg['having']['filter']['bool']['must'][]=$term;
                            }else{
                                $term['match_phrase'][$termk]['query']=$tmp_da_str;
                                $this->havingagg['having']['filter']['bool']['must'][]=$term;
                            }
                                unset($term['match_phrase']);
                        }
                        $this->tmp_lock_str_have=$lowerstr;
                    }
                    $this->tmp_lock_have=$lowerstr;
                    $this->tmp_str_have=$lowerstr;
                    break;
                case 'in':
                    if(strtolower($arr[$i-1]['base_expr'])=='not'){
                        break;
                    }
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_have!='' && $this->tmp_lock_have==$lowerstr){
                        if($this->tmp_str_filter_have==''){
                            $this->count_tmp_filter_have++;
                        }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                            $this->count_tmp_filter_have++;
                        }
                    }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                            $this->count_tmp_filter_have++;
                    }
                    if(isset($arr[$i+1]['sub_tree']) && !empty($arr[$i+1]['sub_tree'])){
                        foreach ($arr[$i+1]['sub_tree'] as &$vv) {
                            if(!is_numeric($vv['base_expr']) && $this->version_es=='5.x'){
                                $termk .='.keyword';
                            }
                            $tmp_da_str=str_replace('"','',$vv['base_expr']);
                            $tmp_da_str=str_replace("'","",$tmp_da_str);
                            $this->havingagg['having']['filter']['terms'][$termk][]=$tmp_da_str;
                        }
                    }
                    $this->tmp_lock_have=$lowerstr;
                    $this->tmp_str_have=$termk;
                    unset($termk);
                    break;
                case 'not':
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_have!='' && $this->tmp_lock_have==$lowerstr){
                        if($this->tmp_str_filter_have==''){
                            $this->count_tmp_filter_have++;
                        }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                            $this->count_tmp_filter_have++;
                        }
                    }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                            $this->count_tmp_filter_have++;
                    }
                    if(isset($arr[$i+2]['sub_tree']) && !empty($arr[$i+2]['sub_tree'])){
                        foreach ($arr[$i+2]['sub_tree'] as &$vv) {
                            if(!is_numeric($vv['base_expr']) && $this->version_es=='5.x'){
                                $termk .='.keyword';
                            }
                            $tmp_da_str=str_replace('"','',$vv['base_expr']);
                            $tmp_da_str=str_replace("'","",$tmp_da_str);
                            $this->havingagg['having']['filter']['bool']['must_not']['terms'][$termk][]=$tmp_da_str;
                        }
                    }
                    $this->tmp_lock_have=$lowerstr;
                    $this->tmp_str_have=$termk;
                    unset($termk);
                    break;
                case '>':
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_have!='' && $this->tmp_lock_have==$lowerstr){
                            if($this->tmp_str_filter_have==''){
                                $this->count_tmp_filter_have++;
                            }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        }
                        if(isset($this->havingagg['having']['filter']['bool']['must'][0]) && $this->tmp_lock_fi_have!='' && $this->tmp_lock_fi_have==$lowerstr){
                            if($this->tmp_fi_have==''){
                                $this->count_fi_have++;
                            }else if($this->tmp_fi_have!='' && $this->tmp_fi_have!=$termk){
                                $this->count_fi_have++;
                            }
                        }
                        if(isset($this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][0]) && $this->tmp_lock_range_have!=''){
                            if($this->tmp_str_range_have==''){
                                $this->count_tmp_range_have++;
                            }else if($this->tmp_str_range_have!='' && $this->tmp_str_range_have!=$termk){
                                $this->count_tmp_range_have++;
                            }
                        }
                        $this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['gt']=$tmp_da_str;
                         if(!isset($this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']) && $is_date){
                            $this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']="+08:00";
                        }
                    }else{
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_str_have!='' && $this->tmp_lock_str_have!=$lowerstr){
                            if($this->tmp_str_have==''){
                                $this->count_tmp_have++;
                            }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_have++;
                            }
                        }
                        if(!isset($this->havingagg['having']['filter']['range']) && $this->tmp_lock_have!='' ){
                            if($this->tmp_str_filter_have==''){
                                $this->count_tmp_filter_have++;
                            }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        $this->havingagg['having']['filter']['range'][$termk]['gt']=$tmp_da_str;
                        if(!isset($this->havingagg['having']['filter']['range'][$termk]['time_zone']) && $is_date){
                            $this->havingagg['having']['filter']['range'][$termk]['time_zone']="+08:00";
                        }
                    }
                    $this->tmp_str_have=$termk;
                    $this->tmp_lock_str_have=$lowerstr;
                    $this->tmp_lock_have=$lowerstr;
                    $this->tmp_lock_range_have=$lowerstr;
                    $this->tmp_lock_fi_have=$lowerstr;
                    break;
                case '>=':
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_have!='' && $this->tmp_lock_have==$lowerstr){
                            if($this->tmp_str_filter_have==''){
                                $this->count_tmp_filter_have++;
                            }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        }
                        if(isset($this->havingagg['having']['filter']['bool']['must'][0]) && $this->tmp_lock_fi_have!='' && $this->tmp_lock_fi_have==$lowerstr){
                            if($this->tmp_fi_have==''){
                                $this->count_fi_have++;
                            }else if($this->tmp_fi_have!='' && $this->tmp_fi_have!=$termk){
                                $this->count_fi_have++;
                            }
                        }
                        if(isset($this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][0]) && $this->tmp_lock_range_have!='' ){
                            if($this->tmp_str_range_have==''){
                                $this->count_tmp_range_have++;
                            }else if($this->tmp_str_range_have!='' && $this->tmp_str_range_have!=$termk){
                                $this->count_tmp_range_have++;
                            }
                        }
                        $this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['gte']=$tmp_da_str;
                         if(!isset($this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']) && $is_date){
                            $this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']="+08:00";
                        }
                    }else{
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_str_have!='' && $this->tmp_lock_str_have!=$lowerstr){
                            if($this->tmp_str_have==''){
                                $this->count_tmp_have++;
                            }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_have++;
                            }
                        }
                        if(!isset($this->havingagg['having']['filter']['range']) && $this->tmp_lock_have!='' ){
                            if($this->tmp_str_filter_have==''){
                                $this->count_tmp_filter_have++;
                            }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        $this->havingagg['having']['filter']['range'][$termk]['gte']=$tmp_da_str;
                        if(!isset($this->havingagg['having']['filter']['range'][$termk]['time_zone']) && $is_date){
                            $this->havingagg['having']['filter']['range'][$termk]['time_zone']="+08:00";
                        }
                    }
                    $this->tmp_str_have=$termk;
                    $this->tmp_lock_str_have=$lowerstr;
                    $this->tmp_lock_have=$lowerstr;
                    $this->tmp_lock_range_have=$lowerstr;
                    $this->tmp_lock_fi_have=$lowerstr;
                    break;
                case '<':
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_have!='' && $this->tmp_lock_have==$lowerstr){
                            if($this->tmp_str_filter_have==''){
                                $this->count_tmp_filter_have++;
                            }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        }
                         if(isset($this->havingagg['having']['filter']['bool']['must'][0]) && $this->tmp_lock_fi_have!='' && $this->tmp_lock_fi_have==$lowerstr){
                            if($this->tmp_fi_have==''){
                                $this->count_fi_have++;
                            }else if($this->tmp_fi_have!='' && $this->tmp_fi_have!=$termk){
                                $this->count_fi_have++;
                            }
                        }
                        if(isset($this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][0]) && $this->tmp_lock_range_have!=''){
                            if($this->tmp_str_range_have==''){
                                $this->count_tmp_range_have++;
                            }else if($this->tmp_str_range_have!='' && $this->tmp_str_range_have!=$termk){
                                $this->count_tmp_range_have++;
                            }
                        }
                        $this->havingagg['having']['filter']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['lt']=$tmp_da_str;
                         if(!isset($this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']) && $is_date){
                            $this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']="+08:00";
                        }
                    }else{
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_str_have!='' && $this->tmp_lock_str_have==$lowerstr){
                            if($this->tmp_str_have==''){
                                $this->count_tmp_have++;
                            }
                        }
                        if(!isset($this->havingagg['having']['filter']['range']) && $this->tmp_lock_have!='' ){
                            if($this->tmp_str_filter_have==''){
                                $this->count_tmp_filter_have++;
                            }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        $this->havingagg['having']['filter']['range'][$termk]['lt']=$tmp_da_str;
                        if(!isset($this->havingagg['having']['filter']['range'][$termk]['time_zone']) && $is_date){
                            $this->havingagg['having']['filter']['range'][$termk]['time_zone']="+08:00";
                        }
                    }
                    
                    $this->tmp_str_have=$termk;
                    $this->tmp_lock_str_have=$lowerstr;
                    $this->tmp_lock_have=$lowerstr;
                    $this->tmp_lock_range_have=$lowerstr;
                    $this->tmp_lock_fi_have=$lowerstr;
                    break;
                case '<=':
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_have!='' && $this->tmp_lock_have==$lowerstr){
                            if($this->tmp_str_filter_have==''){
                                $this->count_tmp_filter_have++;
                            }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        }
                        if(isset($this->havingagg['having']['filter']['bool']['must'][0]) && $this->tmp_lock_fi_have!='' && $this->tmp_lock_fi_have==$lowerstr){
                            if($this->tmp_fi_have==''){
                                $this->count_fi_have++;
                            }else if($this->tmp_fi_have!='' && $this->tmp_fi_have!=$termk){
                                $this->count_fi_have++;
                            }
                        }
                        if(isset($this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][0]) && $this->tmp_lock_range_have!='' ){
                            if($this->tmp_str_range_have==''){
                                $this->count_tmp_range_have++;
                            }else if($this->tmp_str_range_have!='' && $this->tmp_str_range_have!=$termk){
                                $this->count_tmp_range_have++;
                            }
                        }
                        $this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['lte']=$tmp_da_str;
                         if(!isset($this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']) && $is_date){
                            $this->havingagg['having']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']="+08:00";
                        }
                    }else{
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_str_have!='' && $this->tmp_lock_str_have==$lowerstr){
                            if($this->tmp_str_have==''){
                                $this->count_tmp_have++;
                            }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_have++;
                            }
                        }
                        if(!isset($this->havingagg['having']['filter']['range']) && $this->tmp_lock_have!=''){
                            if($this->tmp_str_filter_have==''){
                                $this->count_tmp_filter_have++;
                            }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        $this->havingagg['having']['filter']['range'][$termk]['lte']=$tmp_da_str;
                        if(!isset($this->havingagg['having']['filter']['range'][$termk]['time_zone']) && $is_date){
                            $this->havingagg['having']['filter']['range'][$termk]['time_zone']="+08:00";
                        }
                    }
                    $this->tmp_str_have=$termk;
                    $this->tmp_lock_str_have=$lowerstr;
                    $this->tmp_lock_have=$lowerstr;
                    $this->tmp_lock_range_have=$lowerstr;
                    $this->tmp_lock_fi_have=$lowerstr;
                    break;
                case 'like':
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    $tmp_la_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_la_str=str_replace("'","",$tmp_la_str);
                    if(isset($arr[$i+2]['base_expr']) && strtolower($arr[$i+2]['base_expr'])=='or' || isset($arr[$i-2]['base_expr']) && strtolower($arr[$i-2]['base_expr'])=='or'){
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_have!='' && $this->tmp_lock_have!=$lowerstr){
                            if($this->tmp_str_filter_have==''){
                                $this->count_tmp_filter_have++;
                            }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                $this->count_tmp_filter_have++;
                            }
                        }
                        if(isset($this->havingagg['having']['filter']['bool']['must'][0]) && $this->tmp_lock_fi_have!='' && $this->tmp_lock_fi_have!=$lowerstr){
                            if($this->tmp_fi_have==''){
                                $this->count_fi_have++;
                            }else if($this->tmp_fi_have!='' && $this->tmp_fi_have!=$termk){
                                $this->count_fi_have++;
                            }
                        }
                         if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                            //$term['match_phrase'][$termk.'.keyword']=str_replace("%","",$tmp_la_str);
                            $term['wildcard'][$termk.'.keyword']=str_replace("%","*",$tmp_la_str);
                            $this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][]=$term;
                        }else{
                            //$term['match_phrase'][$termk]=str_replace("%","",$tmp_la_str);\
                            $term['wildcard'][$termk]=str_replace("%","*",$tmp_la_str);
                            $this->havingagg['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][]=$term;
                        }
                    }else{
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_str_have!='' && $this->tmp_lock_str_have!=$lowerstr){
                            if($this->tmp_str_have==''){
                                $this->count_tmp_have++;
                            }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                                $this->count_tmp_have++;
                            }
                        }
                        if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_have!='' && $this->tmp_lock_have!=$lowerstr){
                                if($this->tmp_str_filter_have==''){
                                    $this->count_tmp_filter_have++;
                                }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                                    $this->count_tmp_filter_have++;
                                }
                            }
                        if(!is_numeric($arr[$i+1]['base_expr']) && $this->version_es=='8.x'){
                            //$term['match_phrase'][$termk.'.keyword']=str_replace("%","",$tmp_la_str);
                            //wildcard
                            $term['wildcard'][$termk.'.keyword']=str_replace("%","*",$tmp_la_str);
                            $this->havingagg['having']['filter']['must'][$this->count_tmp_have]['bool']['must'][]=$term;
                        }else{
                            //$term['match_phrase'][$termk]=str_replace("%","",$tmp_la_str);
                            $term['wildcard'][$termk]=str_replace("%","*",$tmp_la_str);
                            $this->havingagg['having']['filter']['bool']['must'][]=$term;
                        }
                    }
                    unset($term['wildcard']);
                    $this->tmp_lock_str_have=$lowerstr;
                    $this->tmp_lock_have=$lowerstr;
                    $this->tmp_lock_fi_have=$lowerstr;
                    break;
                case 'between':
                     if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_str_have!='' && $this->tmp_lock_str_have!=$lowerstr){
                        if($this->tmp_str_have==''){
                            $this->count_tmp_have++;
                        }else if($this->tmp_str_have!='' && $this->tmp_str_have!=$termk){
                            $this->count_tmp_have++;
                        }
                    }
                    if(isset($this->havingagg['having']['filter']) && $this->tmp_lock_have!='' && $this->tmp_lock_have!=$lowerstr){
                        if($this->tmp_str_filter_have==''){
                            $this->count_tmp_filter_have++;
                        }else if($this->tmp_str_filter_have!='' && $this->tmp_str_filter_have!=$termk){
                            $this->count_tmp_filter_have++;
                        }
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $is_date=strtotime($tmp_da_str)?strtotime($tmp_da_str):false;
                    $this->havingagg['having']['filter']['range'][$termk]['gte']=$tmp_da_str;
                    if(!isset($this->havingagg['having']['filter']['range'][$termk]['time_zone']) && $is_date){
                        $this->havingagg['having']['filter']['range'][$termk]['time_zone']="+08:00";
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+3]['base_expr']);
                    $tmp_da_str=str_replace("'","",$tmp_da_str);
                    $this->havingagg['having']['filter']['range'][$termk]['lte']=$tmp_da_str;
                    $this->tmp_str_have=$termk;
                    $this->tmp_lock_str_have=$lowerstr;
                    $this->tmp_lock_have=$lowerstr;
                    break;
            }
    }





    private function having($arr){
        if(isset($this->parsed['HAVING']) && !empty($this->parsed['HAVING'])){
            for ($i=0; $i <count($arr);$i++) { 
                $this->haveext($arr,$i);
            }
            if(isset($this->parsed['GROUP']) && !empty($this->parsed['GROUP'])){
            }else{
                $this->Builderarr['aggs']['having']=$this->havingagg['having'];
            }
        }
    }

    private function select($arr){
        if(isset($this->parsed['GROUP']) && !empty($this->parsed['GROUP'])){
        }else{
            $tmp_source=array();
            foreach ($arr as $k => $v) {
                if($v['expr_type']=='aggregate_function'){
                     if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                        $term_tmp_arrs=explode(".",$v['sub_tree'][0]['base_expr']);
                        if($term_tmp_arrs[1]=='*'){
                            continue;
                        }
                        if($term_tmp_arrs[1]!='keyword'){
                            array_push($tmp_source,$term_tmp_arrs[1]);
                            if(isset($v['alias']['name'])){
                                $this->Builderarr['aggs'][$v['alias']['name']]['stats']['field']=$term_tmp_arrs[1];
                            }else{
                                $this->Builderarr['aggs'][$v['sub_tree'][0]['base_expr']]['stats']['field']=$term_tmp_arrs[1];
                            }
                        }else{
                            array_push($tmp_source,$v['sub_tree'][0]['base_expr']);
                            if(isset($v['alias']['name'])){
                                $this->Builderarr['aggs'][$v['alias']['name']]['cardinality']['field']=$v['sub_tree'][0]['base_expr'];
                            }else{
                                $this->Builderarr['aggs'][$v['sub_tree'][0]['base_expr']]['cardinality']['field']=$v['sub_tree'][0]['base_expr'];
                            }
                       }                        
                    }else{
                        if($v['sub_tree'][0]['base_expr']=='*'){
                            continue;
                        }
                        array_push($tmp_source,$v['sub_tree'][0]['base_expr']);
                        if(isset($v['alias']['name'])){
                            $this->Builderarr['aggs'][$v['alias']['name']]['stats']['field']=$v['sub_tree'][0]['base_expr'];
                        }else{
                            $this->Builderarr['aggs'][$v['sub_tree'][0]['base_expr']]['stats']['field']=$v['sub_tree'][0]['base_expr'];
                        }  
                    }
                }else{
                    array_push($tmp_source,$v['base_expr']);
                }
            }
            if(!empty($tmp_source)){
                $this->Builderarr['_source']['include']=$tmp_source;
            }
        }        
    }

    private function updateset($arr){
        foreach ($arr as &$v) {
            if($v['sub_tree']){
                $tmp_sub[$v['sub_tree'][0]['base_expr']]=$v['sub_tree'][2]['base_expr'];
                $this->Builderarr['doc']=$tmp_sub;
                unset($tmp_sub);
            }
        }
    }
}
?>
