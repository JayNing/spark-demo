package com.ning.hadoop.customrecordreader.my;

import com.ning.hadoop.customrecordreader.parsencbi.custom.FastaLineReader;
import com.ning.hadoop.customrecordreader.parsencbi.model.BigText;
import com.ning.hadoop.customrecordreader.parsencbi.util.SequenceConstants;
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
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * ClassName: MyCustomRecordReader
 * Description:
 * date: 2020/11/3 14:59
 *
 * @author ningjianjian
 */
public class MyCustomRecordReader extends RecordReader<BigText, BigText> {

    private static final Logger logger = LoggerFactory.getLogger(MyCustomRecordReader.class);

    private long start;
    private long pos;
    private long end;
    private FastaLineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private BigText key;
    private BigText value;
    private int maxLineLength ;
    private byte[] recordDelimiterBytes;
    private boolean isCompressedInput;

    public MyCustomRecordReader(byte[] recordDelimiter){
        this.recordDelimiterBytes = recordDelimiter;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration job = taskAttemptContext.getConfiguration();
        this.maxLineLength = 2 * 1024 * 1024;
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

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
        if (key == null){
            key = new BigText();
        }
        if (value == null){
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
