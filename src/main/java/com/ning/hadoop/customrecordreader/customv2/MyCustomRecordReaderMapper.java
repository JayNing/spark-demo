package com.ning.hadoop.customrecordreader.customv2;

import com.ning.hadoop.customrecordreader.parsencbi.model.BigText;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: MyCustomRecordReaderMapper
 * Description:
 * date: 2020/11/3 15:16
 *
 * @author ningjianjian
 */
public class MyCustomRecordReaderMapper extends Mapper<BigText, BigText, BigText, BigText> {

    @Override
    protected void map(BigText key, BigText value, Context context) throws IOException,
            InterruptedException {
        // 直接将读取的记录写出去
        context.write(key, value);
    }


}
