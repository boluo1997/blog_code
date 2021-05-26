package tuling.net;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * 首先 URL restURL = new URL(url);这其中的url就是需要调的目标接口地址,URL类是java.net.*下的类，这个不陌生。
 * setRequestMethod("POST");请求方式是有两个值进行选择，一个是GET,一个是 POST，选择对应的请求方式
 * setDoOutput(true);setDoInput(true);
 * setDoInput(): 设置是否向httpUrlConnection输出，因为这个是 post请求，参数要放在http正文内，因此需要设为true, 默认是false;
 * setDoOutput(): 设置是否从httpUrlConnection读入，默认情况下是 true;
 * setAllowUserInteraction(); allowUserInteraction 如果为 true，则在允许用户交互（例如弹出一个验证对话框）的上下文中对此 URL 进行检查。
 */
public class MallTest {

	public String load(String url) throws Exception {

		URL restUrl = new URL(url);

		HttpURLConnection conn = (HttpURLConnection) restUrl.openConnection();
		conn.setRequestMethod("POST");
		conn.setDoOutput(true);
		conn.setAllowUserInteraction(false);

		BufferedReader bReader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		String line, resultStr = "";

		while (null != (line = bReader.readLine())) {
			System.out.println("join ...");
			resultStr += line;
		}

		System.out.println("123456---" + resultStr);
		bReader.close();
		return resultStr;
	}

	public static void main(String[] args) throws Exception {
		MallTest mallTest = new MallTest();
		String resultString = mallTest.load("https://search.jd.com/Search?keyword=%E6%89%8B%E6%9C%BA&" +
				"enc=utf-8&wq=%E6%89%8B%E6%9C%BA&pvid=aa6d38253ae24fb5accb7dc02b21f069");
		System.out.println(resultString);
	}
}

















