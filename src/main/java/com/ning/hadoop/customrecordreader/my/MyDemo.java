package com.ning.hadoop.customrecordreader.my;

import com.ning.hadoop.customrecordreader.parsencbi.custom.CombineSequenceJob;
import com.ning.hadoop.customrecordreader.parsencbi.custom.FastaFileInputFormat;
import com.ning.hadoop.customrecordreader.parsencbi.custom.FastaRecordReader;
import com.ning.hadoop.customrecordreader.parsencbi.model.BigText;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * ClassName: MyDemo
 * Description:
 * date: 2020/11/3 14:58
 *
 * @author ningjianjian
 */
public class MyDemo {

//    private final static String INPUT_PATH = "D:\\Project\\test\\BIO\\NCBI\\nr\\input\\nrtest.gz";
    private final static String INPUT_PATH = "C:\\Users\\ningjianjian\\Desktop\\custom\\input.txt";
    private final static String OUTPUT_PATH = "C:\\Users\\ningjianjian\\Desktop\\custom\\output";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        File inputFile = new File(INPUT_PATH);

        //1、配置
        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(inputFile.toURI(),conf);

        if(fileSystem.exists(new Path(OUTPUT_PATH)))
        {
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        Job job = Job.getInstance(conf, "Define RecordReader");

        //2、打包运行必须执行的方法
        job.setJarByClass(MyCustomRecordReader.class);

        //3、输入路径
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));

        //设置输入格式
        job.setInputFormatClass(MyCustomInputFormat.class);

        job.setMapOutputKeyClass(BigText.class);
        job.setMapOutputValueClass(BigText.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BigText.class);

        //4、Map
        job.setMapperClass(MyCustomRecordReaderMapper.class);

        //6、Reducer
        job.setReducerClass(MyCustomRecordReaderReduce.class);
//        job.setNumReduceTasks(2);//reduce个数默认是1

        job.setOutputFormatClass(TextOutputFormat.class);

        //7、 输出路径
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        //8、提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
