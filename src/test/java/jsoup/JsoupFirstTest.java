package jsoup;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Set;

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
        String content = FileUtils.readFileToString(new File("doc/test/jsoup-test.html"), "utf-8");

        // 解析字符串
        Document doc = Jsoup.parse(content);

        String title = doc.getElementsByTag("title").first().text();
        System.out.println(title);

    }

    @Test
    public void testFile() throws Exception {

        // 解析文件
        Document doc = Jsoup.parse(new File("doc/test/jsoup-test.html"), "utf-8");

        String title = doc.getElementsByTag("title").first().text();
        System.out.println(title);

    }

    @Test
    public void testDom() throws Exception {

        // 解析文件, 获取document对象
        Document doc = Jsoup.parse(new File("doc/test/jsoup-test.html"), "utf-8");

        // 获取元素

        // 1. 根据id来获取元素
        Element element1 = doc.getElementById("people");
        System.out.println("获取到的元素内容是: " + element1.text());

        // 2.根据标签来获取元素
        Element element2 = doc.getElementsByTag("span").first();
        System.out.println("获取到的元素内容是: " + element2.text());

        // 3.根据class来获取元素
        Element element3 = doc.getElementsByClass("all a_js on").first();
        System.out.println("获取到的元素内容是: " + element3.text());

        // 4.根据属性来获取元素
        Element element4 = doc.getElementsByAttribute("abc").first();
        Element element5 = doc.getElementsByAttributeValue("abc", "456").first();
        System.out.println("获取到的元素内容是: " + element4.text());
        System.out.println("获取到的元素内容是: " + element5.text());

    }

    @Test
    public void testData() throws Exception {

		// 解析文件, 获取document
		Document doc = Jsoup.parse(new File("doc/test/jsoup-test.html"), "utf-8");

		// 根据id获取元素
		Element element = doc.getElementById("age1");

		String str = "";
		// 从元素中获取数据

		// 1. 从元素中获取id
		str = element.id();

		// 2.从元素中获取className
		str = element.className();

		Set<String> set = element.classNames();
		for(String s : set){
			System.out.println(s);
		}

		// 3.从元素中获取属性的值
		str = element.attr("id");

		// 4.从元素中获取所有属性attributes
		Attributes attributes = element.attributes();
		System.out.println(attributes.toString());

		// 5.从元素汇总获取文本内容text
		str = element.text();

		System.out.println(str);
	}


	@Test
	public void testSelector() throws Exception {
		// 解析文件, 获取document
		Document doc = Jsoup.parse(new File("doc/test/jsoup-test.html"), "utf-8");

		// 1.通过标签查找元素
		Elements elements1 = doc.select("span");
		for(Element e : elements1){
			System.out.println(e.text());
		}

		// 2.通过id查找元素
		Elements elements2 = doc.select("#people");
		for(Element e : elements2){
			System.out.println(e.text());
		}

		// 3.通过class名称查找元素
		Elements elements3 = doc.select(".test_class");
		for(Element e : elements3){
			System.out.println(e.text());
		}

		// 4.通过属性来获取
		Element elements4 = doc.select("[abc]").first();
		System.out.println(elements4.text());

		// 5.利用属性值来查找元素
		Element elements5 = doc.select("[abc=123]").first();
		System.out.println(elements5.text());

    }

	@Test
	public void testSelector2() throws Exception {

    	// 解析文件, 获取document
		Document doc = Jsoup.parse(new File("doc/test/jsoup-test.html"), "utf-8");

		// 1. 元素+ID 查找元素
		Elements elements1 = doc.select("h3#boluo");
		for(Element e : elements1){
			System.out.println(e.text());
		}

		// 2. 元素+class
		Elements elements2 = doc.select("h3.class_boluo");
		for(Element e : elements2){
			System.out.println(e.text());
		}

		// 3.元素+属性名
		Elements elements3 = doc.select("span[abcd]");
		for(Element e : elements3){
			System.out.println(e.text());
		}

		// 4.任意组合
		Elements elements4 = doc.select("span[abcd].sname");
		for(Element e : elements4){
			System.out.println(e.text());
		}

		// 查找某个元素下的子元素 .class li
		Elements elements5 = doc.select(".sname li");
		for(Element e : elements5){
			System.out.println(e.text());
		}

		// 查找某个父元素下的直接子元素
		Elements elements6 = doc.select(".sname > ul > li");
		for(Element e : elements6){
			System.out.println(e.text());
		}

		// 查找某个父元素下所有直接子元素
		Elements elements7 = doc.select(".sname > ul > *");
		for(Element e : elements7){
			System.out.println(e.text());
		}


	}

}
