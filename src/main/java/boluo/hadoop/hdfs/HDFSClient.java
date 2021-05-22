package boluo.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class HDFSClient {

    public void put() throws Exception {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "boluo");

        fileSystem.copyFromLocalFile(new Path("d:\\1.txt"), new Path("/"));
        fileSystem.close();
    }

}
