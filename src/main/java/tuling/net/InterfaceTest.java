package tuling.net;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

public class InterfaceTest {

	/**
	 * 调用对方接口方法
	 *
	 * @param path 第三方提供的路径
	 * @param data 向第三方发送的数据, 大多数情况下发送 json让对方解析
	 */
	public static void interfaceUtil(String path, String data) throws Exception {
		URL url = new URL(path);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		PrintWriter out = null;

		//最常用的Http请求无非是get和post, get请求可以获取静态页面, 也可以把参数放在URL字串后面, 传递给servlet
		//post与get的不同之处在于post的参数不是放在URL字串里面, 而是放在http请求的正文内

		conn.setRequestProperty("accept", "*/*");
		conn.setRequestProperty("connection", "Keep-Alive");
		conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)");

		//设置是否向httpUrlConnection输出, 设置是否从httpUrlConnection读入, 此外发送post请求必须设置这两个
		conn.setDoOutput(true);
		conn.setDoInput(true);

		// 获取URLConnection对象对应的输出流
		out = new PrintWriter(conn.getOutputStream());

		// 发送请求数据即数据
		out.print(data);

		// 缓冲数据
		out.flush();

		// 获取URLConnection对象对应的输入流
		InputStream is = conn.getInputStream();

		// 构造一个字符流缓存
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String str = "";
		while ((str = br.readLine()) != null) {
			System.out.println(str);
		}

		// 关闭流
		is.close();

		//断开连接, 最好写上, disconnect是在底层tcp socket链接空闲时才切断, 如果正在被其他线程使用就不切断
		//固定多线程的话, 如果不disconnect, 链接会增多, 直到收发不出信息, 写上disconnect后正常一些
		conn.disconnect();
		System.out.println("完整结束!");
	}

	public static void main(String[] args) throws Exception {
		interfaceUtil("http://api.map.baidu.com/telematics/v3/weather?location=嘉兴&output=json&ak=5slgyqGDENN7Sy7pw29IUvrZ", "");
	}

}





