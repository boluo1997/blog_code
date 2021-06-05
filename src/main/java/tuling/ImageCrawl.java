package tuling;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class ImageCrawl {

    private static String url = "http://www.nipic.com/topic/show_27202_1.html";

    public static void main(String[] args) throws IOException {
        // 1.apacheHttpClient();
        // 2.jsoup包的使用
        Document document = Jsoup.connect(url).get();
        Elements elements = document.select("li.new-search-works-item");
        for (int i = 0; i < elements.size(); i++) {
            Elements imgElement = elements.get(i).select("a > img");
            Connection.Response response = Jsoup.connect("http:" + imgElement.attr("src"))
                    .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36")
                    .ignoreContentType(true)
                    .execute();

            String name = imgElement.attr("alt");
            ByteArrayInputStream stream = new ByteArrayInputStream(response.bodyAsBytes());
            FileUtils.copyInputStreamToFile(stream, new File("D://fileitem//" + name + i + ".png"));
        }

    }

    private static void apacheHttpClient() {
        HttpClient client = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        // 伪装成浏览器-用户代理
        httpGet.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36");
        HttpResponse response = null;

        try {
            response = client.execute(httpGet);
            HttpEntity entity = response.getEntity();
            System.out.println(EntityUtils.toString(entity));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }
}
