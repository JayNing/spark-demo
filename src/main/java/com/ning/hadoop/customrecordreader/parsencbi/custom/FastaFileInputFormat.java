package com.ning.hadoop.customrecordreader.parsencbi.custom;

import com.google.common.base.Charsets;
import com.ning.hadoop.customrecordreader.parsencbi.model.BigText;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Descibe file format to Hadoop
 *
 * Extends from InputFormat
 *
 * @author: Arthur Hu
 * @date: 2018/1/3 下午4:12
 * Description:
 */
public class FastaFileInputFormat extends FileInputFormat<BigText,BigText> {

    private static final Logger logger = LoggerFactory.getLogger(FastaFileInputFormat.class);

    private static final double SPLIT_SLOP = 1.1;

    private long unitsPerSplit=1000;

    private long splitSizeLimit = 128*1024*1024;

    @Override
    public RecordReader<BigText, BigText> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        String delimiter = ">";

        return new FastaRecordReader(delimiter.getBytes(Charsets.UTF_8));
    }

    public boolean shouldIgnore(String fileName) {
        return false;
    }

    /**
     * split file into InputSplit
     * @param job
     * @return
     * @throws IOException
     */
    @Override
   public List<InputSplit> getSplits(JobContext job) throws IOException{

        StopWatch sw = new StopWatch().start();
        // generate splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus> files = listStatus(job);
        for (FileStatus file: files) {
            if (shouldIgnore(file.getPath().getName())) {
                logger.error("expect to only read patent fasta file: '{}' will be ignored", file.getPath().getName());
                continue;
            }
            splits.addAll(getSplitForFile(file,job.getConfiguration(),this.unitsPerSplit));
        }
        sw.stop();
        return splits;

    }


    private List<FileSplit> getSplitForFile(FileStatus file,
                                            Configuration conf, long unitsInSplit) throws IOException {
        Path path = file.getPath();
        List<FileSplit> splits = new ArrayList<>();

        FileSystem  fs = path.getFileSystem(conf);

        LineReader lineReader = null;

        try{

            FSDataInputStream in  = fs.open(path);
            lineReader = new FastaLineReader(in,conf);

            Text line = new Text();
            int numLines = 0;
            long begin = 0;
            long length = 0;
            int num = -1;

            while ((num = lineReader.readLine(line)) > 0) {
                numLines++;
                length += num;
                if (length>=splitSizeLimit) {
                    splits.add(createFileSplit(path, begin, length));
                    begin += length;
                    length = 0;
                    numLines = 0;
                }
            }

            if (numLines != 0) {
                splits.add(createFileSplit(path, begin, length));
            }
        }catch (Exception e){
            logger.error("getSplitForFile@FastaInputFormat, split file failed.",e);
        }finally {
            if (lineReader != null) {
                lineReader.close();
            }
        }
        return splits;
    }


    protected FileSplit createFileSplit(Path fileName, long begin, long length) {
        return (begin == 0)
                ? new FileSplit(fileName, begin, length - 1, new String[] {})
                : new FileSplit(fileName, begin - 1, length, new String[] {});
    }


    @Override
    public boolean isSplitable(JobContext context, Path file){
        return true;
    }

    public long getUnitsPerSplit() {
        return unitsPerSplit;
    }

    public void setUnitsPerSplit(long unitsPerSplit) {
        this.unitsPerSplit = unitsPerSplit;
    }
}
