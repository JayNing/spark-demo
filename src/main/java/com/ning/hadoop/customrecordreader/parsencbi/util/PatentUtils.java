package com.ning.hadoop.customrecordreader.parsencbi.util;

import com.ning.hadoop.customrecordreader.parsencbi.model.BigText;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: Arthur Hu
 * @date: 2018/1/5 下午4:41
 * Description:
 */
public class PatentUtils {

    private static final Logger logger = LoggerFactory.getLogger(PatentUtils.class);

    private static final String SEQUENCE_SPLIT2 = "sequence|_sequence_|_from_patent_";


    public static String formatPN(String patentId) {
        if (StringUtils.isEmpty(patentId)) {
            return null;
        }
        patentId = patentId.toUpperCase();
        if (patentId.contains("_")) {
            patentId = patentId.replaceAll("_", "");
        }

        if (patentId.contains("-")) {
            patentId = patentId.replaceAll("-", "");
        }

        if (patentId.startsWith("US")) {
            if (patentId.substring(patentId.length() - 1).matches("[A-Za-z]{1}")) {
                patentId = patentId.substring(0, patentId.length() - 1);
            } else if (patentId.startsWith("US") && patentId.substring(patentId.length() - 2).matches("[B-Zb-z]{1}[0-9]{1}")) {
                patentId = patentId.substring(0, patentId.length() - 2);
            }
        }

        return patentId;
    }

    public static String formatPNForBigText(BigText patentId) throws Exception {
        if (patentId == null || patentId.isEmpty()) {
            return null;
        }
        patentId = patentId.toUpperCase();
        if (patentId.contains("_")) {
            patentId = patentId.replaceAll("_", "");
        }

        if (patentId.contains("-")) {
            patentId = patentId.replaceAll("-", "");
        }

        if (patentId.startsWith("US")) {

            if (patentId.startsWith("US0")) {
                patentId = patentId.replace("US0", "US");
            }

            BigText lastChar = patentId.substring(patentId.getTextLength() - 1);

            if (lastChar.toString().matches("[A-Za-z]{1}")) {
                patentId = patentId.substring(0, patentId.getTextLength() - 1);
            } else if (patentId.startsWith("US") && patentId.substring(patentId.getTextLength() - 2).toString().matches("[B-Zb-z]{1}[0-9]{1}")) {
                patentId = patentId.substring(0, patentId.getTextLength() - 2);
            }
        }

        return patentId.toString();
    }


    public static boolean checkPN(String sequenceHeader) {
        return SequencePatentUtils.isPatentSequence(sequenceHeader);

    }

    public static boolean checkPNForBigText(BigText sequenceHeader) {
        return checkPN(sequenceHeader.toString());
    }


}
