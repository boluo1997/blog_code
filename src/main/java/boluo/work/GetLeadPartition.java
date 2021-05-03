package boluo.work;

import com.clearspring.analytics.util.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GetLeadPartition {

    public static void main(String[] args) throws IOException {
        // spark初始化
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Simple Application")
                .getOrCreate();

        String path = "file:///D:/data/base2";
        Instant ts = Timestamp.valueOf(LocalDateTime.of(2021, 3, 5, 9, 42)).toInstant();
        System.out.println(getLeadPartition(path, "hash", ts));
    }

    public static String getLeadPartition(String path, String part, Instant ts) throws IOException {

        List<String> results = Lists.newArrayList();
        Path parentPath = new Path(path);

        FileSystem fs = FileSystem.get(parentPath.toUri(), SparkSession.active().sparkContext().hadoopConfiguration());
        FileStatus[] fsParentFiles = fs.listStatus(parentPath);
        List<FileStatus> fsParentLists = Arrays.asList(fsParentFiles);
        Collections.sort(fsParentLists);
        for (FileStatus fileParent : fsParentLists) {

            if (fileParent.getPath().toString().contains(part + "=__HIVE_DEFAULT_PARTITION__")) {
                continue;
            }
            int parentNameIndex = fileParent.getPath().toString().lastIndexOf("/");
            String parentName = fileParent.getPath().toString().substring(parentNameIndex + 1);

            if (parentName.startsWith(part + "=")) {

                // 遍历目录下每个文件
                FileStatus[] fsFiles = fs.listStatus(fileParent.getPath());
                for (FileStatus fileChild : fsFiles) {

                    // 如果找到文件修改时间大于传入时间的情况
                    if (ts.compareTo(Instant.ofEpochMilli(fileChild.getModificationTime())) < 0) {
                        int index = fileParent.getPath().toString().indexOf("=");
                        String pathName = fileParent.getPath().toString().substring(index + 1);
                        results.add(pathName);
                        break;
                    }
                }
            }
        }

        if (results.size() == 0) {
            return null;
        } else {
            // 字符串排序, 返回第一个
            Collections.sort(results);
            return results.get(0);
        }
    }
}
