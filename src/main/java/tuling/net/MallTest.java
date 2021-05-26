package tuling.net;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

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

















