package com.ning.hadoop.customrecordreader.parsencbi.custom;


import com.ning.hadoop.customrecordreader.parsencbi.model.BigText;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The record reader which read fasta file which is extended from RecordReader
 *
 * @author: Arthur Hu
 * @date: 2018/1/3 下午5:21
 * Description:
 */
public class FastaRecordReader extends RecordReader<BigText,BigText> {

    private static final Logger logger = LoggerFactory.getLogger(FastaRecordReader.class);

    public static final String MAX_LINE_LENGTH =
            "mapreduce.input.linerecordreader.line.maxlength";

    private long start;
    private long pos;
    private long end;
    private FastaLineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private int maxLineLength ;
    private BigText key;
    private BigText value;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes;
    public static final int DEFAULT_MAX_RECORD_LIMIT = 1 * 1024 * 1024;//1MB



    public FastaRecordReader(byte[] recordDelimiter){
        this.recordDelimiterBytes = recordDelimiter;
    }


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration job = taskAttemptContext.getConfiguration();
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        this.maxLineLength = DEFAULT_MAX_RECORD_LIMIT;

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        fileIn.seek(start);
        in = new FastaLineReader(
                fileIn, job, this.recordDelimiterBytes);
        filePosition = fileIn;
        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        /**
         * hadoop mapreduce is single thread, so can reuse the key and value instances
         * if you want to implement this reader in mutiple thread sence,
         * please create new instance every time.
         */
        if(key == null){
            key = new BigText();
        }
        if(value == null){
            value = new BigText();
        }

        Text templeValue = new Text();

        int newSize = 0;
        while (getFilePosition() <= end ){
            newSize = in.readLine(templeValue, maxLineLength, maxLineLength);
            value = new BigText(templeValue);
            templeValue.clear();
            pos += newSize;
            if ((newSize == 0) || (newSize < maxLineLength)) {
                break;
            }
        }

        if(value.getTextLength() > 0){

            BigText fastaString = value;

            if(fastaString.startsWith(">")){
                try{
                    int firstLintPos = fastaString.firstIndexOf("\n");
                    int lastLintPos = fastaString.lastIndexOf("\n");

                    if(firstLintPos>-1){
                        key = fastaString.substring(0, firstLintPos);
                        if(lastLintPos > firstLintPos){
                            value = fastaString.substring(firstLintPos+1,lastLintPos);
                        }else{
                            value = fastaString.substring(firstLintPos+1);
                        }
                    }else{
                        logger.error("nextKeyValue@FastaRecordReader, data format error.");
                    }
                }catch (Exception e){
                    System.out.print(fastaString);
                    logger.error("nextKeyValue@FastaRecordReader, get next record error.",e);
                }
            }else if(newSize>0 && !fastaString.startsWith(">")){
                logger.error("nextKeyValue@FastaRecordReader, split error, the first character is not '>' , position="+pos);
            }
        }

        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public BigText getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BigText getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }
}
