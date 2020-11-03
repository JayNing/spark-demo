package com.ning.hadoop.customrecordreader.my;

import com.ning.hadoop.customrecordreader.parsencbi.model.BigText;
import com.ning.util.GsonUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: MyCustomRecordReaderMapper
 * Description:
 * date: 2020/11/3 15:16
 *
 * @author ningjianjian
 */
public class MyCustomRecordReaderReduce extends Reducer<BigText, BigText, NullWritable, Text> {

    @Override
    protected void reduce(BigText key, Iterable<BigText> values, Reducer<BigText, BigText, NullWritable, Text>.Context context) throws IOException,
            InterruptedException {

        Text text = new Text();

        BigText value = values.iterator().next();

        String sequenceValue = value.getStringValue();

        if (value != null && StringUtils.isNotBlank(sequenceValue)){
            sequenceValue = sequenceValue.replaceAll("\r","").replaceAll("\n", "").trim();
        }

        String giListString = key.getStringValue();
        if (key != null && StringUtils.isNotBlank(giListString)){
            String[] giArray = giListString.split("\u0001");
            List<String> giList = new ArrayList<>();
            for (String giA : giArray) {
                String gi = giA.split(" ")[0].replace(">", "").replace("\r", "").trim();
                giList.add(gi);
            }
            NcbiParseBean bean = new NcbiParseBean();
            bean.setSeq_val(sequenceValue);
            bean.setGi_list(giList);

            text.set(GsonUtils.toJsonString(bean));
        }

        context.write(null, text);
    }


}
