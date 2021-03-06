package heima.test;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HttpParamPostTest {

    public static void main(String[] args) throws Exception {

        // 创建HttpClient对象
        CloseableHttpClient httpClient = HttpClients.createDefault();

        // 创建HttpPost对象, 设置url地址
        HttpPost httpPost = new HttpPost("http://resource.ityxb.com/booklist/find.html");

        // 创建List集合, 封装表单中的参数
        List<NameValuePair> param = new ArrayList<>();

        // 设置请求地址: http://resource.ityxb.com/booklist/find.html?search=Java
        param.add(new BasicNameValuePair("search", "Java"));

        // 创建表单的Entity对象
        UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(param, "utf-8");

        // 设置表单的Entity对象到POST请求中
        httpPost.setEntity(formEntity);

        CloseableHttpResponse response = null;

        try {
            // 使用HttpClient发起请求, 获取response
            response = httpClient.execute(httpPost);
            // 解析响应
            if (response.getStatusLine().getStatusCode() == 200) {
                String content = EntityUtils.toString(response.getEntity(), "utf-8");
                System.out.println(content);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
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
