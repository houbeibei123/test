package com.rmzx.java;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.*;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

public class HuaixinInvoke implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(HuaixinInvoke.class);

    private static final int timeOut = 10 * 1000;



    public static  CloseableHttpClient httpClient(String requestUrl) {

        String hostname = requestUrl.split("/")[2];
        int port = 80;
        if (hostname.contains(":")) {
            String[] arr = hostname.split(":");
            hostname = arr[0];
            port = Integer.parseInt(arr[1]);
        }
        return createHttpClient(200, 40, 100, hostname, port);

    }

    private static  CloseableHttpClient createHttpClient(int maxTotal, int maxPerRoute, int maxRoute, String hostname, int port) {
        ConnectionSocketFactory plainsf = PlainConnectionSocketFactory
                .getSocketFactory();
        LayeredConnectionSocketFactory sslsf = SSLConnectionSocketFactory
                .getSocketFactory();
        Registry<ConnectionSocketFactory> registry = RegistryBuilder
                .<ConnectionSocketFactory>create().register("http", plainsf)
                .register("https", sslsf).build();
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(
                registry);
        // 将最大连接数增加
        cm.setMaxTotal(maxTotal);
        // 将每个路由基础的连接增加
        cm.setDefaultMaxPerRoute(maxPerRoute);
        HttpHost httpHost = new HttpHost(hostname, port);
        // 将目标主机的最大连接数增加
        cm.setMaxPerRoute(new HttpRoute(httpHost), maxRoute);

        // 请求重试处理
        HttpRequestRetryHandler httpRequestRetryHandler = (exception, executionCount, context) -> {
            if (executionCount >= 5) {// 如果已经重试了5次，就放弃
                return false;
            }
            if (exception instanceof NoHttpResponseException) {// 如果服务器丢掉了连接，那么就重试
                return true;
            }
            if (exception instanceof SSLHandshakeException) {// 不要重试SSL握手异常
                return false;
            }
            if (exception instanceof InterruptedIOException) {// 超时
                return false;
            }
            if (exception instanceof UnknownHostException) {// 目标服务器不可达
                return false;
            }
            if (exception instanceof ConnectTimeoutException) {// 连接被拒绝
                return false;
            }
            if (exception instanceof SSLException) {// SSL握手异常
                return false;
            }

            HttpClientContext clientContext = HttpClientContext
                    .adapt(context);
            HttpRequest request = clientContext.getRequest();
            // 如果请求是幂等的，就再次尝试
            if (!(request instanceof HttpEntityEnclosingRequest)) {
                return true;
            }
            return false;
        };

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .setRetryHandler(httpRequestRetryHandler).build();

        return httpClient;
    }


    public static String get(CloseableHttpClient httpClient,String url, String id, String title, String content, String articleUrl, String pubTime, String source) throws URISyntaxException, IOException {
        URIBuilder builder = new URIBuilder(url);
        builder.addParameter("content", content);
        builder.addParameter("id", id);
        builder.addParameter("title", title);
        builder.addParameter("url", articleUrl);
        builder.addParameter("publishTime", pubTime);
        builder.addParameter("source", source);
        HttpGet httpget = new HttpGet(builder.build());
        config(httpget);
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpget, HttpClientContext.create());
            HttpEntity entity = response.getEntity();
            String result = EntityUtils.toString(entity, "utf-8");
            EntityUtils.consume(entity);
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Get请求失败!!!");
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private static  void config(HttpRequestBase httpRequestBase) {
        // 配置请求的超时设置
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(timeOut)
                .setConnectTimeout(timeOut).setSocketTimeout(timeOut).build();
        httpRequestBase.setConfig(requestConfig);
    }


    public static void main(String[] args) throws Exception {
        String relevance = "";
        for (int i = 0; i < 3; i++) {
            CloseableHttpClient closeableHttpClient = HuaixinInvoke.httpClient("http://101.200.60.206:80/api/v1/selection");
            relevance = HuaixinInvoke.get(closeableHttpClient,"http://101.200.60.206:80/api/v1/selection",
                    "-2651766568882013671",
                    "傲视众生_饺子媛",
                    "㋈ //@柒悅Ava:#成毅梦醒长安# 不惧朝堂风云，不怕阴谋陷阱，不忘壮志本心。期待和炎炎子的早日相见！！哥哥好好休息！可以穿着花衬衣度个假了哈！@成毅 //@成毅:#梦醒长安杀青特辑#杀青，再见！梦回长安处，依然似少年。||分享一组新鲜出炉的#梦醒长安特辑# [兔子][兔子][兔子]《梦醒长安》由@成毅 @张予曦 @韩栋 @宣璐 @何晟铭 领衔主演，讲述了“少年天子”李炎踌躇满志，在危机四伏的长安与“第一女保镖”程若鱼携手作战，拨开迷雾的故事。期待早日荧屏相见[兔子][兔子][兔子]http://t.cn/A6buqr05",
                    "http://fgw.linyi.gov.cn/info/1536/7956.htm",
                    "2020-09-18 10:09:01",
                    "新浪微博"
            );
            if (StringUtils.isNotBlank(relevance)) {
                if (StringUtils.isNotBlank(relevance) && relevance.contains("rs")) {
                    String rs = JSON.parseObject(relevance).getString("rs");
                    System.out.println(rs);
                }
                break;
            }
        }
    }
}
