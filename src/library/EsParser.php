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
    private $table;
    private $agg;
    private $sort;
    private $count_tmp=0;
    private $tmp_str='';
    private $fistgroup='';
    private $limit;
    public $result;
    public $explain;
    /**
     * Constructor. It simply calls the parse() function. 
     * Use the public variable $parsed to get the output.
     * 
     * @param String  $sql           The SQL statement.
     * @param boolean $calcPositions True, if the output should contain [position], false otherwise.
     */
    public function __construct($sql = false,$calcPositions = false,$url='http://127.0.0.1:9200') {
        $this->url=$url;
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
        if(isset($this->parsed['SELECT']) && !empty($this->parsed['SELECT'])){
            $this->select($this->parsed['SELECT']);
        }
        if(isset($this->parsed['FROM']) && !empty($this->parsed['FROM'])){
            $this->table($this->parsed['FROM']);
        }
        //limit
        if(isset($this->parsed['LIMIT']) && !empty($this->parsed['LIMIT'])){
            $this->limit($this->parsed['LIMIT']);
            if(isset($this->parsed['GROUP']) && !empty($this->parsed['GROUP'])){
            	$this->Builderarr['size']=0;
            }else{
            	$this->Builderarr['from']=$this->limit['from'] * $this->limit['size'];
            	$this->Builderarr['size']=$this->limit['size'];
        	}
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
        //request
        return $this->PostEs($this->Builderarr);
    }

    public function explain(){
    	$this->explain=json_encode($this->Builderarr,true);
    	return $this->explain;
    }


    private function table($arr){
    	foreach ($arr as $v) {
    		if($v['table']){
    			$this->table=$v['table'];
    			$this->url .="/".$v['table']."/".$v['table']."/_search?pretty";
    		}
    	}
    }

    private function PostEs($postdata,$json=false,$token=false){
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
	                'Content-Type: application/json; charset=utf-8',
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
        }else{
            $total_str=$output['hits']['total'];
            if(isset($this->parsed['GROUP']) && !empty($this->parsed['GROUP'])){
                if(empty($output['aggregations'][$this->fistgroup]['buckets'])){
                    $outputs['result']=$output['aggregations'][$this->fistgroup]['buckets'];
                }else{
                    $outputs['result']=array_slice($output['aggregations'][$this->fistgroup]['buckets'],-$this->limit['size']);
                }
            }else{
                $outputs['result']=$output['hits']['hits'];
            }
            $outputs['total']=$total_str;
            $this->result=json_encode($outputs,true);
        }
        return $this->result;
	    
    }

    private function where($arr){
    	for($i=0;$i<count($arr);$i++){
    		if(!is_numeric($arr[$i]['base_expr'])){
    			$lowerstr = strtolower($arr[$i]['base_expr']);
    		}else{
    			$lowerstr = $arr[$i]['base_expr'];
    		}
    		switch ($lowerstr) {
    			case '=':
    				if(strrpos($arr[$i-1]['base_expr'],".")){
    					$term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
    					$termk=$term_tmp_arr[1];
    				}else{
    					$termk=$arr[$i-1]['base_expr'];
    				}
    				if(!is_numeric($arr[$i+1]['base_expr'])){
    					$term['term'][$termk.'.keyword']=$arr[$i+1]['base_expr'];
    					$this->Builderarr['query']['bool']['must'][0]['bool']['must'][]=$term;
    				}else{
    					$term['term'][$termk]=$arr[$i+1]['base_expr'];
    					$this->Builderarr['query']['bool']['must'][0]['bool']['must'][]=$term;
    				}
    				unset($term['term']);
    				break;
    			case 'in':
    				if(strrpos($arr[$i-1]['base_expr'],".")){
    					$term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
    					$termk=$term_tmp_arr[1];
    				}else{
    					$termk=$arr[$i-1]['base_expr'];
    				}
    				if(isset($arr[$i+1]['sub_tree']) && !empty($arr[$i+1]['sub_tree'])){
    					foreach ($arr[$i+1]['sub_tree'] as &$vv) {
    						if(!is_numeric($vv['base_expr'])){
    							$termk .='.keyword';
    						}
    						$this->Builderarr['query']['bool']['filter']['terms'][$termk][]=$vv['base_expr'];
    					}
    				}
    				unset($termk);
    				break;
    			case '>':
    				if(strrpos($arr[$i-1]['base_expr'],".")){
    					$term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
    					$termk=$term_tmp_arr[1];
    				}else{
    					$termk=$arr[$i-1]['base_expr'];
    				}
                    if(isset($this->Builderarr['query']['bool']['must'][0])){
                        if($this->tmp_str==''){
                            $this->count_tmp++;
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                            $this->count_tmp++;
                        }
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['gt']=$tmp_da_str;
                    if(!isset($this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['time_zone'])){
                        $this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['time_zone']="+08:00";
                    }
                    $this->tmp_str=$termk;
    				break;
                case '>=':
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    if(isset($this->Builderarr['query']['bool']['must'][0])){
                        if($this->tmp_str==''){
                            $this->count_tmp++;
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                            $this->count_tmp++;
                        }
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['gte']=$tmp_da_str;
                    if(!isset($this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['time_zone'])){
                        $this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['time_zone']="+08:00";
                    }
                    $this->tmp_str=$termk;
                    break;
    			case '<':
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    if(isset($this->Builderarr['query']['bool']['must'][0])){
                        if($this->tmp_str==''){
                            $this->count_tmp++;
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                            $this->count_tmp++;
                        }
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['lt']=$tmp_da_str;
                    if(!isset($this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['time_zone'])){
                        $this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['time_zone']="+08:00";
                    }
                    $this->tmp_str=$termk;
                    break;
                case '<=':
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    if(isset($this->Builderarr['query']['bool']['must'][0])){
                        if($this->tmp_str==''){
                            $this->count_tmp++;
                        }else if($this->tmp_str!='' && $this->tmp_str!=$termk){
                            $this->count_tmp++;
                        }
                    }
                    $tmp_da_str=str_replace('"','',$arr[$i+1]['base_expr']);
                    $this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['lte']=$tmp_da_str;
                    if(!isset($this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['time_zone'])){
                        $this->Builderarr['query']['bool']['must'][$this->count_tmp]['range'][$termk]['time_zone']="+08:00";
                    }
                    $this->tmp_str=$termk;
                    break;
                case 'like':
                    if(strrpos($arr[$i-1]['base_expr'],".")){
                        $term_tmp_arr=explode(".",$arr[$i-1]['base_expr']);
                        $termk=$term_tmp_arr[1];
                    }else{
                        $termk=$arr[$i-1]['base_expr'];
                    }
                    if(!is_numeric($arr[$i+1]['base_expr'])){
                        $term['wildcard'][$termk.'.keyword']=str_replace("%","*",$arr[$i+1]['base_expr']);
                        $this->Builderarr['query']['bool']['must'][0]['bool']['must'][]=$term;
                    }else{
                        $term['wildcard'][$termk]=str_replace("%","*",$arr[$i+1]['base_expr']);
                        $this->Builderarr['query']['bool']['must'][0]['bool']['must'][]=$term;
                    }
                    unset($term['wildcard']);
                    break;
    		}
    	}
    }

    private function groupby($arr){
    	$aggs= array();
    	for ($j=0; $j <count($arr); $j++) { 
    		if(strrpos($arr[$j]['base_expr'],".")){
    			$term_tmp_arr=explode(".",$arr[$j]['base_expr']);
    			$termk=$term_tmp_arr[1];
    			$termk_tmp=$termk;
    		}else{
    			$termk=$arr[$j]['base_expr'];
    			$termk_tmp=$termk;
    		}
            $tmmp=0;
            if(!is_numeric($termk)){
                $termk .='.keyword';
            }
            if(isset($this->fistgroup) && $this->fistgroup==''){
                $this->fistgroup=$termk_tmp.'_group';
            }
            if(isset($this->parsed['SELECT']) && !empty($this->parsed['SELECT'])){
                foreach ($this->parsed['SELECT'] as $v) {
                    $this->top_hits=1;
                    if(strrpos($v['base_expr'],"*")){
                        //$this->top_hits=1;
                    }else{
                        if($v['expr_type']=='aggregate_function'){
                            $lowerstr = strtolower($v['base_expr']);
                            switch ($lowerstr) {
                                case 'count':
                                    $agg[$j][$termk_tmp.'_group']['terms']['field']=$termk;
                                    $agg[$j][$termk_tmp.'_group']['terms']['size']=$this->limit['from']*$this->limit['size']==0?10:$this->limit['from']*$this->limit['size'];
                                    if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                                        $term_tmp_arrs=explode(".",$v['sub_tree'][0]['base_expr']);
                                        $cardinalitys[$v['alias']['name']]['cardinality']['field']=$term_tmp_arrs[1];
                                    }else{
                                        $cardinalitys[$v['alias']['name']]['cardinality']['field']=$v['sub_tree'][0]['base_expr'];
                                    }
                                    $tmmp=1;
                                    $agggs[$j][$termk_tmp.'_group']['aggs']=$cardinalitys;
                                    $aggs[$j]=array_merge_recursive($agg[$j], $agggs[$j]);
                                    break;
                                case 'sum':
                                    $agg[$j][$termk_tmp.'_group']['terms']['field']=$termk;
                                    $agg[$j][$termk_tmp.'_group']['terms']['size']=$this->limit['from']*$this->limit['size']==0?10:$this->limit['from']*$this->limit['size'];
                                    if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                                        $term_tmp_arrs=explode(".",$v['sub_tree'][0]['base_expr']);
                                        $cardinalitys[$v['alias']['name']]['sum']['field']=$term_tmp_arrs[1];
                                    }else{
                                        $cardinalitys[$v['alias']['name']]['sum']['field']=$v['sub_tree'][0]['base_expr'];
                                    }
                                    $tmmp=1;
                                    $agggs[$j][$termk_tmp.'_group']['aggs']=$cardinalitys;
                                    $aggs[$j]=array_merge_recursive($agg[$j], $agggs[$j]);
                                    break;
                                case 'min':
                                    $agg[$j][$termk_tmp.'_group']['terms']['field']=$termk;
                                    $agg[$j][$termk_tmp.'_group']['terms']['size']=$this->limit['from']*$this->limit['size']==0?10:$this->limit['from']*$this->limit['size'];
                                    if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                                        $term_tmp_arrs=explode(".",$v['sub_tree'][0]['base_expr']);
                                        $cardinalitys[$v['alias']['name']]['min']['field']=$term_tmp_arrs[1];
                                    }else{
                                        $cardinalitys[$v['alias']['name']]['min']['field']=$v['sub_tree'][0]['base_expr'];
                                    }
                                    $tmmp=1;
                                    $agggs[$j][$termk_tmp.'_group']['aggs']=$cardinalitys;
                                    $aggs[$j]=array_merge_recursive($agg[$j], $agggs[$j]);
                                    break;
                                case 'max':
                                    $agg[$j][$termk_tmp.'_group']['terms']['field']=$termk;
                                    $agg[$j][$termk_tmp.'_group']['terms']['size']=$this->limit['from']*$this->limit['size']==0?10:$this->limit['from']*$this->limit['size'];
                                    if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                                        $term_tmp_arrs=explode(".",$v['sub_tree'][0]['base_expr']);
                                        $cardinalitys[$v['alias']['name']]['max']['field']=$term_tmp_arrs[1];
                                    }else{
                                        $cardinalitys[$v['alias']['name']]['max']['field']=$v['sub_tree'][0]['base_expr'];
                                    }
                                    $tmmp=1;
                                    $agggs[$j][$termk_tmp.'_group']['aggs']=$cardinalitys;
                                    $aggs[$j]=array_merge_recursive($agg[$j], $agggs[$j]);
                                    break;
                                case 'avg':
                                    $agg[$j][$termk_tmp.'_group']['terms']['field']=$termk;
                                    $agg[$j][$termk_tmp.'_group']['terms']['size']=$this->limit['from']*$this->limit['size']==0?10:$this->limit['from']*$this->limit['size'];
                                    if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                                        $term_tmp_arrs=explode(".",$v['sub_tree'][0]['base_expr']);
                                        $cardinalitys[$v['alias']['name']]['avg']['field']=$term_tmp_arrs[1];
                                    }else{
                                        $cardinalitys[$v['alias']['name']]['avg']['field']=$v['sub_tree'][0]['base_expr'];
                                    }
                                    $tmmp=1;
                                    $agggs[$j][$termk_tmp.'_group']['aggs']=$cardinalitys;
                                    $aggs[$j]=array_merge_recursive($agg[$j], $agggs[$j]);
                                    break;
                            }
                            if(isset($this->parsed['ORDER']) && !empty($this->parsed['ORDER'])){
                                foreach ($this->parsed['ORDER'] as $vv) {
                                    if($vv['base_expr']==$v['alias']['name']){
                                        $aggs[$j][$termk_tmp.'_group']['terms']['order'][$vv['base_expr']]=$vv['direction'];
                                         if(strrpos($v['sub_tree'][0]['base_expr'],".")){
                                            $term_tmp_arrss=explode(".",$v['sub_tree'][0]['base_expr']);
                                            $aggs[$j][$termk_tmp.'_group']['aggs'][$vv['base_expr']][$lowerstr]['field']=$term_tmp_arrs[1];
                                        }else{
                                            $aggs[$j][$termk_tmp.'_group']['aggs'][$vv['base_expr']][$lowerstr]['field']=$v['sub_tree'][0]['base_expr'];
                                        }
                                    }
                                }
                            }
                            if($this->top_hits){
                                $aggs[$j][$termk_tmp.'_group']['aggs']['top']['top_hits']['size']=$this->top_hits;
                            }
                        }
                    }
            }
        }
            if($tmmp==0){
                 $agg[$j][$termk_tmp.'_group']['terms']['field']=$termk;
                 $agg[$j][$termk_tmp.'_group']['terms']['size']=$this->limit['from']*$this->limit['size']==0?10:$this->limit['from']*$this->limit['size'];
                $agggs[$j][$termk_tmp.'_group']['aggs']=array();
                $aggs[$j]=array_merge_recursive($agg[$j], $agggs[$j]);
                unset($aggs[$j][$termk_tmp.'_group']['aggs']);
            }	
        }
    	$this->agg['aggs']=$this->inverted($aggs);
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
                $termk=$term_tmp_arr[1];
            }else{
                $termk=$va['base_expr'];
            }
            $this->sort['sort'][][$termk]['order']=$va['direction'];
        }
    }

    private function limit($arr){
    	if(!$arr['offset']){
    		$this->limit['from']=0;
    	}else{
    		$this->limit['from']=$arr['offset'];
    	}
    	$this->limit['size']=$arr['rowcount'];
    }

    private function inverted($arr){
    	for($i=count($arr)-1;$i>=0;$i--){
			if($i>0){
				$arr[$i-1]['aggs']=$arr[$i];
			}
		}
		if(empty($arr)){
			return array();
		}else{
			return $arr[0];
		}
    }

    private function select($arr){
    	
    }








}
?>
