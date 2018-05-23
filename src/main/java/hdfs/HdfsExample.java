package hdfs;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;

public class HdfsExample {
    static {
        System.setProperty("HADOOP_USER_NAME", "wuxb");
        System.setProperty("hadoop.home.dir", "D:\\study\\hadoop-2.7.2");
    }

    @Test
    public void testPrintFilesInfo() throws IOException {
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            Path myPath = new Path("/");
            fs = FileSystem.get(conf);
            FileStatus[] fileStatuses = fs.listStatus(myPath);
            for (FileStatus fileStatus : fileStatuses) {
                String result = Joiner.on(' ').join(
                        fileStatus.getPermission().toString(),
                        fileStatus.getReplication(),
                        fileStatus.getOwner(),
                        fileStatus.getGroup(),
                        fileStatus.getModificationTime(),
                        fileStatus.getPath().toString());
                System.out.println(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fs != null)
                fs.close();
        }
    }

    @Test
    public void testCreatFileOnHDFS() throws IOException {
        String message = "This is a test!";
        byte[] bytes = message.getBytes();
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            Path myPath = new Path("/user/wxb/test.txt");
            fs = FileSystem.get(conf);
            FSDataOutputStream fsDataOutputStream = fs.create(myPath);
            fsDataOutputStream.write(bytes, 0, bytes.length);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fs != null)
                fs.close();
        }
    }

    @Test
    public void testReadFileFromHDFS() throws IOException {
        FileSystem fs = null;
        byte[] bytes = new byte[1024];
        try {
            Configuration conf = new Configuration();
            Path myPath = new Path("/user/wxb/test.txt");
            fs = FileSystem.get(conf);
            FSDataInputStream fsDataInputStream = fs.open(myPath);
            int len = 0;
            while ((len = fsDataInputStream.read(bytes)) != -1) {
                String message = new String(bytes, 0, len);
                System.out.println(message);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fs != null)
                fs.close();
        }
    }
}
