package com.ning.spark.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;

import java.util.*;

/**
 * ClassName: SparkTransformationDemo
 * Description:
 * date: 2020/10/29 10:44
 *
 * @author ningjianjian
 */
public class SparkTransformationTest {

    // Initializing Spark
    private static SparkConf conf = new SparkConf().setAppName("local").setMaster("local");
    private static JavaSparkContext sc = new JavaSparkContext(conf);

    /**
     *
     * mapPartitionsWithIndex函数作用同mapPartitions，不过提供了两个参数，第一个参数为分区的索引。
     * 可以在分区内部进行函数处理时，获取到当前分区的索引
     */
    @Test
    public void mapPartitionsWithIndex(){
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        // numSlices为设置的分区数量
        JavaRDD<Integer> distData = sc.parallelize(data,5);
        //index 为分区索引
        JavaRDD<String> rdd = distData.mapPartitionsWithIndex((Function2<Integer, Iterator<Integer>, Iterator<String>>) (index, input) -> {
            System.out.println("mapPartitionsWithIndex is called....................");
            List<String> list = new ArrayList<>();

            int sum = 0;
            while (input.hasNext()) {
                Integer next = input.next();
                sum += next;
            }
            list.add(index + "|" + sum);

            return list.iterator();
        }, true);

        System.out.println(rdd.collect());

    }

    /***
     * mapPartitions是map的一个变种。map的输入函数是应用于RDD中每个元素，而mapPartitions的输入函数是应用于每个分区，也就是把每个分区中的内容作为整体来处理的
     *
     * def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
     *
     * f即为输入函数，它处理每个分区里面的内容。每个分区中的内容将以Iterator[T]传递给输入函数f，f的输出结果是Iterator[U]。
     * 最终的RDD由所有分区经过输入函数处理后的结果合并起来的。
     */
    @Test
    public void mapPartitions(){
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        // numSlices为设置的分区数量
        JavaRDD<Integer> distData = sc.parallelize(data,5);
        JavaRDD<Integer> rdd = distData.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) in -> {
            System.out.println("mapPartitions is called....................");
            List<Integer> list = new ArrayList<>();
            int sum = 0;
            while (in.hasNext()) {
                Integer next = in.next();
                sum += next;
            }
            list.add(sum);
            return list.iterator();
        });

        System.out.println(rdd.collect());

    }

    /**
     * flatMap(func):跟map(func)类似，但是每个输入项和成为0个或多个输出项
     *
     * Flatmap 和map 区别
     * map(func)函数会对每一条输入进行指定的func操作，然后为每一条输入返回一个对象；而flatMap(func)也会对每一条输入进行执行的func操作，
     * 然后每一条输入返回一个相对，但是最后会将所有的对象再合成为一个对象；从返回的结果的数量上来讲，map返回的数据对象的个数和原来的输入数据是相同的，
     * 而flatMap返回的个数则是不同的。
     *
     * map操作：有map（一条对一条），mapToPair（map成键值对），flatMap（一条记录变n条（n>=0））
     *
     */
    @Test
    public void flatMap(){
        List<String> data = Arrays.asList("Af3234tet5","Berytery574","C4t5y35e");
        JavaRDD<String> distData = sc.parallelize(data);
        System.out.println("process before:" + distData.collect());
        //简单方式
//        JavaRDD<String> map = distData.flatMap(s -> Arrays.asList(s.split("t")).iterator());

        JavaRDD<String> map = distData.flatMap((FlatMapFunction<String, String>) s -> {
            return Arrays.asList(s.split("t")).iterator();
        });
        System.out.println("map.count():" + map.count());
        System.out.println("process after:" + map.collect());
        //支持类型转换，例如返回String[]
        JavaRDD<String[]> map1 = distData.flatMap((FlatMapFunction<String, String[]>) s -> Collections.singletonList(s.split("t")).iterator());
        System.out.println("map1.count():" + map1.count());
        System.out.println("map1 process after :" + map1.collect());

        //支持类型转换，例如返回Map
        JavaRDD<Map<String, String[]>> map2 = distData.flatMap((FlatMapFunction<String, Map<String, String[]>>) s -> {
                Map<String, String[]> mp = new HashMap<>();
                mp.put(s, s.split("t"));
                return Collections.singletonList(mp).iterator();
            }
        );
        System.out.println("map2.count():" + map2.count());
        System.out.println("map2 process after :" + map2.collect());
    }

    @Test
    public void filter(){
        /**
         * .filter(x -> { 判断条件 })
         * 通过传入的每一个元素，返回所有符合判断条件的数据，组成新的RDD
         */
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        System.out.println("process before:" + distData.collect());
        JavaRDD<Integer> map = distData.filter(x -> x > 3);
        System.out.println("process after:" + map.collect());
    }

    /**
     * map(func):将原数据的每个元素传给函数func进行格式化，返回一个新的分布式数据集
     */
    @Test
    public void map(){
        /**
         * .map(x -> { to do ; return })
         * 通过传入的每一个元素，返回一个流程处理后的新元素
         */
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        System.out.println("process before:" + distData.collect());
        JavaRDD<Integer> map = distData.map(x -> {
            System.out.println(x);
            return x * 10;
        });
        System.out.println("map.count():" + map.count());
        System.out.println("process after:" + map.collect());
    }



}
