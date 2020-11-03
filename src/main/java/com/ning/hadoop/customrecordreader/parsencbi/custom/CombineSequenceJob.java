package com.ning.hadoop.customrecordreader.parsencbi.custom;

import com.ning.hadoop.customrecordreader.parsencbi.model.BigText;
import com.ning.hadoop.customrecordreader.parsencbi.util.PatentUtils;
import com.ning.hadoop.customrecordreader.parsencbi.util.SequenceConstants;
import com.ning.hadoop.customrecordreader.parsencbi.util.SequenceHashUtils;
import lombok.SneakyThrows;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: Arthur Hu
 * @date: 2018/1/7 下午10:41
 * Description:
 * reorganize the sequence data for next step(combine and remove duplicated)
 * <p>
 * Mapper:
 * the source data will be :
 * key:  original fasta header + SequenceConstants.HEADER_HASH_ID_SPLITOR + taxonomyId
 * value: the original sequence value
 * <p>
 * output from mapper:
 * key:  original fasta header + SequenceConstants.HEADER_HASH_ID_SPLITOR + taxonomyId
 * value: the original sequence value
 */
public class CombineSequenceJob {

    private static final Logger logger = LoggerFactory.getLogger(CombineSequenceJob.class);

    public static class PreprocessMapper extends Mapper<BigText, BigText, BigText, BigText> {

        /**
         * convert sequence header to sequenceHash then write to mapper output
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(BigText key, BigText value,
                           Context context) {

            try {
                if (key.isEmpty()) {
                    return;
                }

                if (key.contains("\n")) {
                    key = key.replaceAll("\n", "");
                }

                String sequenceHash = SequenceHashUtils.getTaxIdFromHeader(key);

                if (sequenceHash == null) {
                    return;
                }

                // source data example
                // key:  original fasta header + SequenceConstants.HEADER_HASH_ID_SPLITOR + taxonomyId
                // value: the original sequence value
                BigText originalHeader = key.substring(0, key.firstIndexOf(SequenceConstants.HEADER_HASH_ID_SPLITOR));

                BigText newValue = originalHeader.append(SequenceConstants.HEADER_VALUE_SPLITER);
                newValue.append(value);
                value.clear();
                key.clear();
                key.append(sequenceHash);
                // generated data example
                // key: taxonomyId
                // value: original fasta header + SequenceConstants.HEADER_VALUE_SPLITER + original sequence value
                context.write(key, newValue);
            } catch (Exception e) {
                logger.error("map@CombineSequenceJob, map task error.", e);
            }
        }


    }


    public static class PreprocessReducer extends Reducer<BigText, BigText, NullWritable, BigText> {
        private static final byte LF = '\n';


        /**
         * ignore duplication
         * header#sequenceHash#id
         * <p>
         * header1 + HEADER_HEADER_SPLITOR + header2 + [HEADER_HEADER_SPLITOR+header_n] + HEADER_HASH_ID_SPLITOR + taxonpmyId + \n + sequence value
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @SneakyThrows
        @Override
        protected void reduce(BigText key, Iterable<BigText> values, Context context
        ) throws IOException, InterruptedException {


            Map<BigText, Integer> headerMap = new HashMap<>();
            BigText headerBuffer = new BigText();
            BigText textValue = null;
            for (BigText value : values) {
                BigText header = getHeaderFromValue(value);
                if (header == null) {
                    continue;
                }
                if (!PatentUtils.checkPNForBigText(header)) {
                    header.clear();
                    header.append(SequenceConstants.NO_PATENT_HEADER);
                }


                int headerValueSpliterLocation = value.firstIndexOf(SequenceConstants.HEADER_VALUE_SPLITER);
                value = value.substring(headerValueSpliterLocation + SequenceConstants.HEADER_VALUE_SPLITER.length());
                headerMap.put(header, 1);
                if (textValue == null) {
                    textValue = value;
                }

            }

            for (Map.Entry<BigText, Integer> entry : headerMap.entrySet()) {
                headerBuffer.append(SequenceConstants.HEADER_HEADER_SPLITOR).append(entry.getKey());
            }

            BigText first = headerBuffer.substring(0, SequenceConstants.HEADER_HEADER_SPLITOR.length());

            if (first.toString().equals(SequenceConstants.HEADER_HEADER_SPLITOR)) {
                headerBuffer = headerBuffer.substring(SequenceConstants.HEADER_HEADER_SPLITOR.length());
                BigText newHeader = new BigText();
                newHeader.append(SequenceConstants.FASTA_START_CHAR).append(headerBuffer);
                headerBuffer = newHeader;
            }

            headerBuffer = headerBuffer.append(SequenceConstants.HEADER_HASH_ID_SPLITOR).append(key).append("\n").append(textValue);

            key.clear();
            key.append(headerBuffer);
            context.write(NullWritable.get(), key);
        }


//        private String getHeaderFromValue(String value) {
//            if (StringUtils.isEmpty(value)) {
//                return null;
//            }
//
//            String[] valueArray = value.split(SequenceConstants.HEADER_VALUE_SPLITER);
//            if (valueArray.length < 2) {
//                return null;
//            }
//
//            String header = valueArray[0];
//            if (header.startsWith(">")) {
//                header = header.replace(">", "");
//            }
//
//            return header;
//        }

        private BigText getHeaderFromValue(BigText value) {
            if (value == null || value.isEmpty()) {
                return null;
            }

            int headerEndIndex = value.firstIndexOf(SequenceConstants.HEADER_VALUE_SPLITER);

            if (headerEndIndex == -1) {
                return null;
            }

            BigText header = value.substring(0, headerEndIndex);
            if (header.startsWith(SequenceConstants.FASTA_START_CHAR)) {
                header = header.replace(SequenceConstants.FASTA_START_CHAR, "");
            }

            return header;
        }

    }
}
