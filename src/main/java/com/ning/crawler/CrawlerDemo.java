package com.ning.crawler;

import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * ClassName: CrawlerDemo
 * Description:
 * date: 2020/11/3 17:02
 *
 * @author ningjianjian
 */
public class CrawlerDemo {

    private FTPClient ftpClient = null;

    public static void main(String[] args) throws Exception {

        CrawlerDemo fdownloader = new CrawlerDemo();
        fdownloader.connect("ftp.ncbi.nlm.nih.gov", "anonymous", "guest");
        String pathname = "/blast/db/FASTA/";
        fdownloader.getFileList(pathname);
    }

    private String formatDateTime(Date date){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = sdf.format(date);
        return format;
    }

    public void connect(String host, String username, String password) throws Exception {

        ftpClient = new FTPClient();
        ftpClient.connect(host);
        int reply = ftpClient.getReplyCode();

        if (!FTPReply.isPositiveCompletion(reply)) {
            ftpClient.disconnect();
            throw new Exception("FTP Server refused to connect!");
        }

        if (!ftpClient.login(username, password)) {
            ftpClient.disconnect();
            throw new Exception("Invalid username or password!");
        }

        ftpClient.enterLocalPassiveMode();
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

    }

    public List<FTPFile> getFileList(String path) throws IOException {

        FTPFile[] ftpFiles = ftpClient.listFiles(path);

        List<FTPFile> retList = new ArrayList<>();
        if (ftpFiles == null || ftpFiles.length == 0) {
            return retList;
        }

        for (FTPFile ftpFile : ftpFiles) {
            System.out.println("fileName : " + ftpFile.getName() + ", timestamp : " + formatDateTime(ftpFile.getTimestamp().getTime()));
            retList.add(ftpFile);
        }

        return retList;

    }


    public File download(String pathname, String localName) throws Exception {

        if (ftpClient.listFiles(new String(pathname.getBytes(), ftpClient.getControlEncoding())).length == 0) {
            throw new Exception("File dose not exist: " + pathname);
        }

        boolean flag = false;

        File outfile = new File(localName);
        InputStream ins = ftpClient.retrieveFileStream(pathname);
        FileUtils.copyInputStreamToFile(ins, outfile);
        return outfile;
    }


}
