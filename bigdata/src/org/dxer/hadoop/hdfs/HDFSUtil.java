package org.dxer.hadoop.hdfs;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

/**
 * 
 * @class HDFSTest
 * @author linghf
 * @version 1.0
 * @since 2016年4月14日
 */
public class HDFSUtil {
    private Configuration conf = null;

    private FileSystem fileSystem = null;

    public HDFSUtil(String url){
        conf = new HdfsConfiguration();
        try {
            fileSystem = FileSystem.get(URI.create(url), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 
     * @param fileName
     * @throws IOException
     */
    public void listFiles(String fileName) throws IOException {
        FileStatus[] fileStatus = fileSystem.listStatus(new Path(fileName));
        for (FileStatus status: fileStatus) {
            if (status.isDirectory()) {
                listFiles(status.getPath().toString());
            } else {
                System.out.println(status.getPath());
            }
        }
    }

    /**
     * 创建目录
     * 
     * @param dir
     */
    public void mkdir(String dir) {
        try {
            fileSystem.mkdirs(new Path(dir));
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 
     */
    public void ls(String file) {
        try {
            boolean exists = fileSystem.exists(new Path(file));
            if (exists) {
                FileStatus status = fileSystem.getFileStatus(new Path(file));
                System.out.println(status.getPath().toString() + ", exist: " + exists + ", is a " +
                                   (status.isFile()? "dir.": "file."));
            } else {
                System.out.println(file + " is not exists.");
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 下载文件
     * 
     * @param hdfsFile
     * @param localFile
     */
    public void download(String hdfsFile, String localFile) {
        DataOutputStream dos = null;
        FSDataInputStream fsds = null;
        try {
            fsds = fileSystem.open(new Path(hdfsFile));

            dos = new DataOutputStream(new FileOutputStream(localFile));
            byte[] b = new byte[1024];
            while (-1 != fsds.read(b)) {
                dos.write(b);
            }
            dos.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (dos != null) {
                    dos.close();
                }
                if (fsds != null) {
                    fsds.close();
                }
                if (fileSystem != null) {
                    fileSystem.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 删除文件
     * 
     * @param file
     */
    public void delete(String file) {
        try {
            fileSystem.delete(new Path(file), false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传文件
     * 
     * @param localFile
     * @param hdfsFile
     */
    public void put(String localFile, String hdfsFile) {
        FSDataOutputStream fsos = null;
        FileInputStream fis = null;
        try {
            fsos = fileSystem.create(new Path(hdfsFile));
            fis = new FileInputStream(new File(localFile));
            byte[] b = new byte[1024];
            while (-1 != fis.read(b)) {
                fsos.write(b);
            }
            fsos.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fsos != null) {
                    fsos.close();
                }

                if (fis != null) {
                    fis.close();
                }

                if (fileSystem != null) {
                    fileSystem.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws IOException {
        HDFSUtil test = new HDFSUtil("hdfs://11.11.11.10:9000/");
        test.listFiles("/");
        // test.mkdir("/xxtest");
        // test.find("/xxtest");
        // test.download("/hadoop-2.6.4.tar.gz", "d:/t.tar.gz");
        // test.put("d:/pimAccess.log.20151221", "/pimAccess.log");
    }
}
