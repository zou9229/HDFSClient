package com.zou.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 客户端代码常用套路
 * 1、获取一个客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 * HDFS zookeeper
 */
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接的集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        //创建一个配置文件
        Configuration configuration = new Configuration();

        configuration.set("dfs.replication","2");
        //用户
        String user = "zou";
        //1.获取到了客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        //3.关闭资源
        fs.close();
    }

    @Test
    public void testmkdir() throws URISyntaxException, IOException, InterruptedException {

        //2.创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan1"));
    }

    //上传

    //参数优先级
    //hdfs-default.xml=>hdfs-site.xml=>在项目资源目录下的配置文件=>代码里的配置
    @Test
    public void testPut() throws IOException {
        //参数解读：
        //参数一：表示删除原数据；
        //参数二：是否允许覆盖；
        //参数三：原数据路径；
        //参数四：目的路径；
        fs.copyFromLocalFile(false,true,new Path("D:\\sunwukong.txt"),new Path("/xiyou/huaguoshan"));
    }

    //文件下载
    @Test
    public void testGet() throws IOException {
        //参数的解读
        //参数一：原文件是否删除；
        //参数二：原文件的路径HDFS；
        //参数三：目标地址路径windows；
        //参数四：是否循环冗余校验
        fs.copyToLocalFile(false,new Path("/xiyou/huaguoshan"),new Path("D:\\"),true);
    }
}
