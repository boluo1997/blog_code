package boluo.basics;

import org.junit.Test;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

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

    @Test
    // 文件输入输出流
    public void func4() throws Exception {
        // 操作磁盘文件的FileInputStream类(读取文件使用)和FileOutputStream类(写入内容使用)
        // FileInputStream是InputStream的子类
        // FileOutputStream是OutputStream的子类

        // FileInputStream类和FileOutputStream类操作的数据单元是一个字节, 如果文件中有中文字符(占两个字节),
        // 那么使用FileInputStream类和FileOutputStream类读写文件会出现乱码

        File file = new File("word.txt");

        // 创建FileOutputStream对象, 向文件中写入数据
        FileOutputStream out = new FileOutputStream(file, true);

        // 定义字符串, 用来存储要写入文件的内容
        String context = "悲欢如雨";

        // 创建byte型数组, 将要写入文件的内容转为字节数组
        byte[] buy = context.getBytes();

        out.write(buy);
        out.close();

        //创建FileInputStream对象, 用来读取文件内容
        FileInputStream in = new FileInputStream(file);
        byte[] byt = new byte[1024];

        // 从文件中读取信息, 并存入字节数组中
        int len = in.read(byt);

        System.out.println("文件中的信息是: " + new String(byt, 0, len));
        in.close();
    }

    // FileReader类和FileWriter类
    @Test
    public void func5() throws Exception {
        // FileReader类和FileWriter类对应了FileInputStream和FileOutputStream类,
        // 其中, 读取文件的是FileReader类, 向文件中写入内容使用的是FileWriter类
        // FileReader类与FileWriter类操作的数据单元是一个字符, 如果文件中有中文字符, 使用该类可以避免乱码

        // 向文件中写入并读取控制台输入的内容
        while (true) {
            File file = new File("word.txt");
            if (!file.exists()) file.createNewFile();
            System.out.println("请输入要执行的操作序号: (1.写入文件, 2.读取文件)");
            Scanner sc = new Scanner(System.in);
            int choice = sc.nextInt();
            switch (choice) {
                case 1:
                    System.out.println("请输入要写入的文件的内容: ");
                    String tempStr = sc.next();
                    FileWriter fw = null;   // 声明字符输出流

                    // 创建可扩展的字符输入流, 向文件中写入新数据时不覆盖已存在的数据
                    fw = new FileWriter(file, true);
                    fw.write(tempStr + "\r\n");
                    fw.close();

                    System.out.println("上述内容已写入到文本文件中");
                    break;
                case 2:
                    if (file.length() == 0) {
                        System.out.println("文本中的字符个数为0!");
                    } else {
                        FileReader fr = new FileReader(file);
                        char[] cbuf = new char[1024];
                        int hasread = -1;

                        while ((hasread = fr.read()) != -1) {
                            System.out.println("文件中的内容为: " + new String(cbuf, 0, hasread));
                        }
                        fr.close(); // 关闭字符输入流
                    }
                    break;

                default:
                    System.out.println("请输入有效数字...");
                    break;
            }
        }
    }

    // 带缓冲的输入输出流
    @Test
    public void func6() throws Exception {
        // BufferInputStream类和BufferOutputStream类
        // BufferedInputStream类可以对所有InputStream的子类进行带缓冲区的包装
        // BufferedOutputStream类中的flush()方法用来把缓冲区中的字节写入到文件中

        // 以字节为单位进行输入和输出
        String[] content = {"boluo, ", "qidai, ", "dingc, "};
        File file = new File("word.txt");

        FileOutputStream fos = new FileOutputStream(file);
        BufferedOutputStream bos = new BufferedOutputStream(fos);

        byte[] bContent1 = new byte[1024];
        for (int i = 0; i < content.length; i++) {
            bContent1 = content[i].getBytes();
            bos.write(bContent1);
        }
        System.out.println("写入成功!");

        bos.close();
        fos.close();

        FileInputStream fis = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fis);

        byte[] bContent2 = new byte[1024];
        int len = bis.read(bContent2);

        System.out.println("文件中的内容是: " + new String(bContent2, 0, len));

        bis.close();
        fis.close();

    }



}





