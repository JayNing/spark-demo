package com.ning.hadoop.customrecordreader.parsencbi.util;

import com.ning.hadoop.customrecordreader.parsencbi.model.BigText;
import net.openhft.hashing.LongHashFunction;
import org.apache.commons.lang.StringUtils;

/**
 * @author: Arthur Hu
 * @date: 2018/1/5 下午2:33
 * Description:
 */
public class SequenceHashUtils {

    private static final byte LF = '\n';

    private static final String POSITIVE = "p";
    private static final String NEGATIVE = "n";

    private static final int NUM_PER_LINE = 60;


    /**
     * Generate the sequence hash
     *
     * @param value
     * @return
     */
    public static String generateSequenceHash(String value) {
        long sequenceXXHash = LongHashFunction.xx().hashChars(value);
        long sequenceFarmHash = LongHashFunction.farmUo().hashChars(value);

        StringBuffer sequencHash = new StringBuffer();

        String xxhashStr = String.valueOf(sequenceXXHash);

        if (sequenceXXHash < 0) {
            xxhashStr = xxhashStr.replace("-", NEGATIVE);
        } else {
            sequencHash.append(POSITIVE);
        }

        sequencHash.append(xxhashStr).append(sequenceFarmHash);

        return sequencHash.toString();
    }


    public static String generateSequenceHashForBigText(BigText value) {

        if (value == null || value.isEmpty()) {
            return "";
        }


        StringBuffer sequenceXXHashFinal = new StringBuffer();
        StringBuffer sequenceFarmHashFinal = new StringBuffer();

        for (String text : value.getText()) {
            long sequenceXXHashItem = LongHashFunction.xx().hashChars(text);
            sequenceXXHashFinal.append(sequenceXXHashItem);
            long sequenceFarmHashItem = LongHashFunction.farmUo().hashChars(text);
            sequenceFarmHashFinal.append(sequenceFarmHashItem);
        }

        long sequenceXXHash = LongHashFunction.xx().hashChars(sequenceXXHashFinal.toString());

        long sequenceFarmHash = LongHashFunction.farmUo().hashChars(sequenceFarmHashFinal.toString());


        StringBuffer sequencHash = new StringBuffer();

        String xxhashStr = String.valueOf(sequenceXXHash);

        if (sequenceXXHash < 0) {
            xxhashStr = xxhashStr.replace("-", NEGATIVE);
        } else {
            sequencHash.append(POSITIVE);
        }

        sequencHash.append(xxhashStr).append("_").append(sequenceFarmHash);

        return sequencHash.toString();
    }

    public static String getTaxIdFromHeader(String header) {
        if (StringUtils.isEmpty(header)) {
            return null;
        }

        String[] strings = header.split(SequenceConstants.HEADER_HASH_ID_SPLITOR);
        if (strings.length < 2) {
            return null;
        }

        return strings[1];
    }


    public static String getTaxIdFromHeader(BigText key) throws Exception {
        if (key == null || key.isEmpty()) {
            return null;
        }

        int taxIdLocation = key.lastIndexOf(SequenceConstants.HEADER_HASH_ID_SPLITOR);
        BigText taxIdBig = key.substring(taxIdLocation + SequenceConstants.HEADER_HASH_ID_SPLITOR.length());

        return taxIdBig.toString();
    }


    public static BigText generateSequenceHashFastaFormat(BigText key, BigText value) throws Exception {

//        String valueStr = CompressUtils.uncompressSequence(value.getBytes());

        BigText keyStr = new BigText();
        keyStr.append(SequenceConstants.FASTA_START_CHAR).append(key).append("\n");

        value = value.replaceAll("\n", "");


        BigText item = new BigText();
        long start = 0;
        long end = NUM_PER_LINE;
        long strLength = value.getTextLength();
        while (end <= strLength) {
            item.append(value.substring(start, end).append("\n"));
            start = end;
            end += NUM_PER_LINE;
        }

        if (start < strLength) {
            item.append(value.substring(start));
        }

        keyStr.append(item);

        return keyStr;
    }


    public static String extractSequenceIDFromHeader(String header) {
        if (StringUtils.isEmpty(header)) {
            return null;
        }

        String[] keyArray = header.split(SequenceConstants.HEADER_HASH_ID_SPLITOR);

        if (keyArray.length < 2) {
            return null;
        }

        return keyArray[1];
    }


    public static BigText extractSequenceIDFromHeader(BigText header) throws Exception {
        if (header == null || header.isEmpty()) {
            return null;
        }

        int idLocation = header.lastIndexOf(SequenceConstants.HEADER_HASH_ID_SPLITOR);

        BigText sequenceId = header.substring(idLocation + SequenceConstants.HEADER_HASH_ID_SPLITOR.length());


        return sequenceId;
    }

    public static String extractSequenceHeaderFromHeader(String header) {
        if (StringUtils.isEmpty(header)) {
            return null;
        }

        String[] keyArray = header.split(SequenceConstants.HEADER_HASH_ID_SPLITOR);

        return keyArray[0];
    }


    public static BigText extractSequenceHeaderFromHeader(BigText header) {
        if (header == null || header.isEmpty()) {
            return null;
        }

        int idLocation = header.lastIndexOf(SequenceConstants.HEADER_HASH_ID_SPLITOR);

        BigText originalHeader = header.substring(0, idLocation);

        return originalHeader;
    }


    public static String extractIdFromHeader(String header) {
        if (StringUtils.isEmpty(header)) {
            return null;
        }

        String[] keyArray = header.split(SequenceConstants.HEADER_HASH_ID_SPLITOR);

        if (keyArray.length < 3) {
            return null;
        }

        return keyArray[2];
    }
}
