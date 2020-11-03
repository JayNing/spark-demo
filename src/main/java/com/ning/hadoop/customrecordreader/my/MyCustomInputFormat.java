package com.ning.hadoop.customrecordreader.my;

import com.google.common.base.Charsets;
import com.ning.hadoop.customrecordreader.parsencbi.model.BigText;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * ClassName: MyCustomInputFormat
 * Description:
 * date: 2020/11/3 15:10
 *
 * @author ningjianjian
 */
public class MyCustomInputFormat extends FileInputFormat<BigText, BigText> {

    @Override
    public RecordReader<BigText, BigText> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        String delimiter = ">";
        return new MyCustomRecordReader(delimiter.getBytes(Charsets.UTF_8));
    }

}
