package tuling;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class RainbowFart {
	public static void main(String[] args) throws Exception {

		String link = "https://chp.shadiao.app/api.php";
		File file = new File("D:/boluo.txt");
		FileWriter writer = new FileWriter(file, true);

		for (int i = 0; i < 10000; i++) {
			URL url = new URL(link);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");

			if (connection.getResponseCode() == 200) {
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(connection.getInputStream()));

				String result = reader.readLine();
				if (result.startsWith("访问太频繁服务器受不了啦") || result.startsWith("Disconnected")) {
					System.out.println(result);
				} else {
					System.out.println(result);
					writer.write(result + "\n");
					writer.close();
				}
			}
		}
	}
}
