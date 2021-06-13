package boluo.basics;


import com.google.common.collect.Maps;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Note14_DownloadMovie {

    public static void main(String[] args) throws Exception {
        final String SCORE_URL = "https://www.xxxx/xxx";// 主页（手工打码）
        final String DOWNLOAD_DIR = "F:/video/";// 下载目录

//        System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2,SSLv3");
//        System.err.println("爬取子页面...");
//        List<String> allArticle = getAllVideoPage(SCORE_URL);
//        System.err.println("爬取视频...");
//        Map<String, String> urlMap = getUrlInSource(allArticle);
//        System.err.println("本次下载文件数量：" + urlMap.size());
//        System.err.println("开始下载...");
//        downloadMovie(DOWNLOAD_DIR, urlMap);

        Map<String, String> hashMap = Maps.newHashMap();
        hashMap.put("测试", "https://d1.xia12345.com/down/201611/ssyy/GqFX8mpW.mp4");
        downloadMovie(DOWNLOAD_DIR, hashMap);
    }


    /**
     * 爬所有视频页 存入一个list
     *
     * @param source 主页
     * @return 视频页 列表
     * @throws Exception
     */
    private static List<String> getAllVideoPage(String source) throws Exception {
        List<String> urls = new ArrayList<>();
        for (int j = 1; j < 5; j++) { // 要爬哪些页数
            String pageUrl = source;
            // 拼接子页url
            pageUrl = pageUrl + "?sort=new&page=" + j;
            URL url = new URL(pageUrl);
            // 连接url
            BufferedReader br = connectURL(url);
            String info = null;
            for (int i = 0; i < 10000; i++) {
                info = br.readLine();
                if (info != null) {// 这里开始根据实际页面上的url进行字符串截取
                    if (info.contains("target=\"_self\"")) {
                        int start = info.indexOf("href") + 6;
                        int end = start + 6;
                        String substring = "https://www.xxx.xxx" + info.substring(start, end);
                        urls.add(substring);
                    }
                }
            }
        }
        return urls;
    }


    /**
     * 获取视频的URL地址和视频名称存入hashMap
     *
     * @param source 视频页 列表
     * @return 视频名称=下载url
     * @throws IOException
     */
    private static Map<String, String> getUrlInSource(List<String> source) throws Exception {

        Map<String, String> hashMap = new HashMap<>();
        for (int j = 0; j < source.size(); j++) {
            String pageUrl = source.get(j);
            URL url = new URL(pageUrl);
            // 连接url
            BufferedReader br = connectURL(url);
            String info = null;
            String title = null;
            // 此处不要==null进行判断，因为网页中有很多行都是null，否则会报java.lang.NullPointerException?
            for (int i = 0; i < 10000; i++) {
                info = br.readLine();
                if (null != info) {// 这里截取视频名称，也是根据页面实际情况
                    if (info.contains("h1 class=\"text-truncate\"")) {
                        int st = info.indexOf("truncate") + 10;
                        int ed = info.lastIndexOf("h1") - 2;
                        title = info.substring(st, ed);
                    }
                    if (info.contains("https://xxx.xxx.xxx/download/mp4")) {// 这里截取视频实际下载url，也是根据页面实际情况
                        int start = info.indexOf("http");
                        int end = info.lastIndexOf("mp4") + 3;
                        String substring = info.substring(start, end);
                        hashMap.put(title, substring);
                    }
                }
            }
        }
        return hashMap;
    }

    /**
     * 开启多线程下载
     *
     * @param DOWNLOAD_DIR
     * @param urlMap
     */
    private static void downloadMovie(final String DOWNLOAD_DIR, Map<String, String> urlMap) {
        ExecutorService es = Executors.newFixedThreadPool(8);
        for (Map.Entry<String, String> entry : urlMap.entrySet()) {
            final String title = entry.getKey();// 视频名称
            final String url = entry.getValue();// 视频url

            es.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        System.out.println("正在下载:    " + title + ".......");
                        File destFile = new File(DOWNLOAD_DIR + title + ".mp4");

                        download(url, destFile);
                        System.out.println("=========> " + title + " 下载完毕!");

                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    /**
     * 通过视频的URL下载该视频并存入本地
     *
     * @param url      视频的URL
     * @param destFile 视频存入的文件夹
     * @throws IOException
     */
    private static void download(String url, File destFile) throws IOException {
        URL videoUrl = new URL(url);

        InputStream is = videoUrl.openStream();
        FileOutputStream fos = new FileOutputStream(destFile);

        int len = 0;
        byte[] buffer = new byte[1024];
        while ((-1) != (len = is.read(buffer))) {
            fos.write(buffer, 0, len);
        }
        fos.flush();

        if (null != fos) {
            fos.close();
        }

        if (null != is) {
            is.close();
        }
    }

    /**
     * 链接url 返回字节流
     *
     * @param url
     * @return
     * @throws Exception
     */
    private static BufferedReader connectURL(URL url) throws Exception {
        // 这里的代理服务器端口号 需要自己配置
        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("127.0.0.1", 7959));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection(proxy);
        // 若遇到反爬机制则使用该方法将程序伪装为浏览器进行访问
        conn.setRequestMethod("GET");
        conn.setRequestProperty("user-agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36");
        BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
        return br;
    }


}
