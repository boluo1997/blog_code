package tuling;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 存储路径
 */
public class UrlPool {
    public static void main(String[] args) {
        getUrl("http://www.nipic.com/");
    }

    private static void getUrl(String baseUrl) {
        Map<String, Boolean> oldMap = new LinkedHashMap<>();
        String oldLinkHost = "";

        Pattern p = Pattern.compile("(https?://)?[^/\\s]*");
        Matcher m = p.matcher(baseUrl);

        if (m.find()) {
            oldLinkHost = m.group();
        }

        oldMap.put(baseUrl, false);
        oldMap = crawlLinks(oldLinkHost, oldMap);
        for (Map.Entry<String, Boolean> mapping : oldMap.entrySet()) {
            System.out.println("链接: " + mapping.getKey());
        }
    }

    private static Map<String, Boolean> crawlLinks(String oldLinkHost, Map<String, Boolean> oldMap) {
        // 开始递归
        // 再定义一个容器, 存放正在遍历中的url, 如果没有遍历过, 合到oldMap中
        Map<String, Boolean> newMap = new LinkedHashMap<>();

        String oldLink = "";
        for (Map.Entry<String, Boolean> mapping : oldMap.entrySet()) {

            System.out.println("链接: " + mapping.getKey() + "---check: " + mapping.getValue());

            if (!mapping.getValue()) {}
                oldLink = mapping.getKey();
                try {
                    URL url = new URL(oldLink);
                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                    connection.setRequestMethod("GET");
                    if (connection.getResponseCode() == 200) {
                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(connection.getInputStream()));

                        Pattern p = Pattern.compile("<a.*?href=[\"']?((https?://)?/?[^\"']+)[\"']?.*?>(.+)</a>");
                        Matcher matcher = null;
                        String line = "";
                        while ((line = reader.readLine()) != null) {
                            matcher = p.matcher(line);
                            if (matcher.find()) {
                                String newLink = matcher.group(1).trim();

                                if (!newLink.startsWith("http")) {
                                    if (newLink.startsWith("/")) {
                                        newLink = oldLinkHost + newLink;
                                    } else {
                                        newLink = oldLinkHost + "/" + newLink;
                                    }
                                }

                                if (newLink.endsWith("/")) {
                                    newLink = newLink.substring(0, newLink.length() - 1);
                                }

                                if (!oldMap.containsKey(newLink) && !newMap.containsKey(newLink) && newLink.startsWith(oldLinkHost)) {
                                    newMap.put(newLink, false);
                                }
                            }
                        }
                    }
                } catch (Exception e) {

                } finally {

                }
            oldMap.replace(oldLink, false, true);
        }

        if (!newMap.isEmpty()) {
            oldMap.putAll(newMap);
            oldMap.putAll(crawlLinks(oldLinkHost, oldMap));
        }

        return oldMap;
    }
}






















