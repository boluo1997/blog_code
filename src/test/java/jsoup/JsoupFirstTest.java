package jsoup;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;

import java.io.File;
import java.net.URL;

public class JsoupFirstTest {

    @Test
    public void testUrl() throws Exception {

        // 解析URL地址, 第一个参数是访问的URL, 第二个参数是访问时的超时时间
        Document doc = Jsoup.parse(new URL("http://www.itcast.cn"), 1000);

        // 使用标签选择器, 获取title标签中的内容
        String title = doc.getElementsByTag("title").first().text();
        System.out.println(title);

    }

    @Test
    public void testString() throws Exception {

        // 使用工具类读取文件, 获取字符串
        String content = FileUtils.readFileToString(new File("D:\\IDEA_projects\\blog_code\\doc\\test\\jsoup-test.html"), "utf-8");

        // 解析字符串
        Document doc = Jsoup.parse(content);

        String title = doc.getElementsByTag("title").first().text();
        System.out.println(title);

    }

    @Test
    public void testFile() throws Exception {

        // 解析文件
        Document doc = Jsoup.parse(new File("D:\\IDEA_projects\\blog_code\\doc\\test\\jsoup-test.html"), "utf-8");

        String title = doc.getElementsByTag("title").first().text();
        System.out.println(title);

    }

}
