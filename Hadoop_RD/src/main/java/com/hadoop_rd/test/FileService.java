package com.hadoop_rd.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class FileService {
    private String HDFSPATH = "";

    public FileService(String HDFSPATH) {
        this.HDFSPATH = HDFSPATH;
    }

    public FileSystem getFileSystem() {

        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(this.HDFSPATH), new Configuration());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return fs;
    }

    public int alterDir(String oldpath, String newpath) {
        int flag = 0;
        FileSystem fileSystem = this.getFileSystem();
        try {
            fileSystem.rename(new Path(oldpath),new Path(newpath));
            flag = 1;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }
    public int createDir(String path)  {
        int flag = 0;
        FileSystem fileSystem = this.getFileSystem();
        try {
            fileSystem.mkdirs(new Path(path));
            flag = 1;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    public int deleteDir(String path) {
        int flag = 0;
        FileSystem fileSystem = this.getFileSystem();
        try {
            fileSystem.delete(new Path(path),true);
            flag = 1;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }
    public boolean exist(String path){

        FileSystem fileSystem = this.getFileSystem();
        try {
            return fileSystem.exists(new Path(path));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

}
