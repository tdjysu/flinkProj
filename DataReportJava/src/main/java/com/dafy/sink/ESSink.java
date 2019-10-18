package com.dafy.sink;

import com.dafy.bean.ReportDeptBean;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ESSink {




    public static ElasticsearchSink.Builder<ReportDeptBean> getReportDeptBeanBuilder() {
        //指定ES地址
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.8.209", 9200, "http"));
        return new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<ReportDeptBean>() {
                    public IndexRequest createIndexRequest(ReportDeptBean element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("eventTime", element.getEventTime()+"");
                        json.put("deptcode",element.getDeptCode());
                        json.put("deptname",element.getDeptName());
                        json.put("busiAreaCode",element.getBusiAreaCode());
                        json.put("busiAreaName",element.getBusiAreaName());
                        json.put("adminAreaCode",element.getAdminAreaCode());
                        json.put("adminAreaName",element.getAdminAreaName());
                        json.put("fundcode",element.getFundcode());
                        json.put("lendCnt",element.getLendCnt()+"");
                        json.put("lamount",element.getLamount()+"");


                        return Requests.indexRequest()
                                .index("intentreport_index")
                                .type("intenttype")
                                .source(json);
                    }
                    @Override
                    public void process(ReportDeptBean element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
    }
}
