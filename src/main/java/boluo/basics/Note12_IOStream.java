package boluo.basics;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Note12_IOStream {

    @Test
    public void func1() throws IOException {
        File file1 = new File("D:/1.txt");
        file1.createNewFile();

        File file2 = new File("D:/io/", "1.txt");
        file2.createNewFile();

        File folder = new File("D:/io-test");
        File file3 = new File(folder, "1.txt");
        file3.createNewFile();

    }

    @Test
    // 创建并获取文件的基本信息
    public void func2() {
        File file = new File("test.txt");
        if (!file.exists()) {
            System.out.println("未在指定目录下找到名为test.txt的文本文件, 正在创建...");
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("文件创建成功!");
        } else {
            System.out.println("找到名为test.txt的文本文件...");
            if (file.isFile() && file.canRead()) {    // 该文件是一个标准文件且可读
                System.out.println("文件可读, 正在读取文件信息...");

                String fileName = file.getName();
                String filePath = file.getAbsolutePath();
                boolean hidden = file.isHidden();
                long len = file.length();   // 文件中的字节数
                long tempTime = file.lastModified();    // 文件的最后修改时间
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date(tempTime);
                String time = sdf.format(date);

                System.out.println("文件名: " + fileName);
                System.out.println("文件绝对路径: " + filePath);
                System.out.println("文件是否是隐藏文件: " + hidden);
                System.out.println("文件中的字节数: " + len);
                System.out.println("文件最后修改时间: " + time);
                file.delete();
                System.out.println("文件已被删除!");
            } else {
                System.out.println("文件不可读!");
            }
        }
    }

    @Test
    // 创建文件夹并在该文件夹下创建10个子文件夹
    public void func3() {
        String path = "C:\\Test";
        for (int i = 0; i < 10; i++) {
            File folder = new File(path + "\\" + i);
            if (!folder.exists()) {
                folder.mkdirs();
            }
        }

        System.out.println("文件夹创建成功, 请打开C盘查看, \nC盘文件及文件夹列表如下: ");
        File file = new File("C:\\");
        File[] files = file.listFiles();
        for (File folder : files) {
            if (folder.isFile()) {
                System.out.println(folder.getName() + " 文件");
            } else {
                System.out.println(folder.getName() + " 文件夹");
            }
        }

    }


}



