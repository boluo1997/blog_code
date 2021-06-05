package heima.test;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class HttpGetTest {
    public static void main(String[] args) throws Exception {

        // 创建HttpClient对象
        CloseableHttpClient httpClient = HttpClients.createDefault();

        // 设置请求地址: http://resource.ityxb.com/booklist/find.html?search=Java
        // 创建URIBuilder
        URIBuilder uriBuilder = new URIBuilder("http://resource.ityxb.com/booklist/find.html");

        // 设置参数
        uriBuilder.setParameter("search", "Java");

        // 创建HttpGet对象, 设置url地址
        HttpGet httpGet = new HttpGet(uriBuilder.build());

        CloseableHttpResponse response = null;
        // 使用HttpClient发起请求, 获取response
        try {

            response = httpClient.execute(httpGet);
            // 解析响应
            if(response.getStatusLine().getStatusCode() == 200){
                String content = EntityUtils.toString(response.getEntity(), "utf-8");
                System.out.println(content);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            // 关闭response
            try {
                response.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
