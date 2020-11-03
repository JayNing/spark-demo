package com.ning.hadoop.customrecordreader.demo;


import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;


/**
 * ClassName: MyRecordReader
 * Description:
 * date: 2020/11/3 14:20
 *
 * @author ningjianjian
 */
public class MyRecordReader {

    public static class DefRecordReader extends RecordReader<LongWritable, Text> {

        private long start;//分片开始位置
        private long end;//分片结束位置
        private long pos;
        private FSDataInputStream fin = null;
        //自定义自己的key与value
        private LongWritable key = null;
        private Text value = null;
        //A class that provides a line reader from an input stream.
        private LineReader reader = null;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)split;
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path path = fileSplit.getPath();//获取输入分片的路径
            Configuration conf = context.getConfiguration();
            //Return the FileSystem that owns this Path.
            FileSystem fs = path.getFileSystem(conf);
            fin = fs.open(path);
            reader = new LineReader(fin);
            pos = 1;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {

            if(key == null){
                key = new LongWritable();
            }
            key.set(pos);//设置key
            if(value == null){
                value = new Text();
            }
            //并没有跨块，跨文件，而是一个文件作为不可分割的
            if(reader.readLine(value)==0){//一次读取行的内容,并设置值
                return false;
            }
            pos++;
            return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        /**
         * Get the progress within the split
         */
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            fin.close();
        }

    }

    public static class MyFileInputFormat extends FileInputFormat<LongWritable, Text>{

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(
                InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {

            return new DefRecordReader();
        }

        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }
    }


    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.write(key, value);
        }
    }


    public static class DefPartitioner extends Partitioner<LongWritable,Text>{

        @Override
        public int getPartition(LongWritable key, Text value, int numPartitions) {
            //判断奇数行还是偶数行，确定分区
            if(key.get()%2==0){
                key.set(1);//偶数行key通通改为1
                return 1;
            }else {
                key.set(0);//奇数行key通通改为0
                return 0;
            }
        }

    }

    //接收来自不同分区的数据
    public static class MyReducer extends Reducer<LongWritable, Text,Text, IntWritable>{
        Text write_key = new Text();
        IntWritable write_value = new IntWritable();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            int sum=0;
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
            if(key.get()==0){
                write_key.set("奇数行之和");
            }else {
                write_key.set("偶数行之和");
            }
            write_value.set(sum);
            context.write(write_key, write_value);
        }
    }


    private final static String INPUT_PATH = "C:\\Users\\ningjianjian\\Desktop\\custom\\input.txt";
    private final static String OUTPUT_PATH = "C:\\Users\\ningjianjian\\Desktop\\custom\\output";

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

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
        job.setJarByClass(MyRecordReader.class);

        //3、输入路径
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));

        //设置输入格式
        job.setInputFormatClass(MyFileInputFormat.class);

        //4、Map
        job.setMapperClass(MyMapper.class);
        //指定map的输出的<k,v>类型,如果<k3,v3>的类型与<k2,v2>的类型一致，那么可以省略。
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //5、Combiner
        //job.setCombinerClass(MyReducer.class);
        job.setPartitionerClass(DefPartitioner.class);

        //6、Reducer
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(2);//reduce个数默认是1

        //如果<k3,v3>的类型与<k2,v2>的类型不一致，要么都省略，要么都要写。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //7、 输出路径
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        //8、提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
