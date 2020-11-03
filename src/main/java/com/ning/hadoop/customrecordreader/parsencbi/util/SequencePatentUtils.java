package com.ning.hadoop.customrecordreader.parsencbi.util;

import com.ning.hadoop.customrecordreader.parsencbi.model.BigText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Liu Jianwei
 * @date 2018/5/25
 */
public class SequencePatentUtils {
    private static final Logger logger = LoggerFactory.getLogger(SequencePatentUtils.class);
    private static final String SEQUENCE_SPLIT2 = "sequence|_sequence_|_from_patent_";

    private static Pattern PATTERIN_NCBI2 = Pattern.compile("(_[a-zA-Z]{2}_)([\\d]{8})");


    public static boolean isPatentSequence(String header) {
        BigText bigText = new BigText(header);
        return isPatentSequence(bigText);
    }

    public static boolean isPatentSequence(BigText header) {
        BigText[] headers = header.split(SequenceConstants.HEADER_HEADER_SPLITOR);
        boolean flag = true;
        for (BigText sequenceHeader : headers) {
            String format = patentFormatType(sequenceHeader);
            flag = !(format.equals(SequencePatentFormat.UNKNOWN));
            if (flag) {
                return flag;
            }
        }
        return flag;
    }

    private static class SequencePatentFormat {
        public static final String RD = "rd";
        public static final String NCBI_1 = "ncbi_1";
        public static final String NCBI_2 = "ncbi_2";
        public static final String UNKNOWN = "unknown";
    }

    public static String patentFormatType(BigText sequenceHeader) {
        sequenceHeader = sequenceHeader.toLowerCase();
        BigText[] array = sequenceHeader.split(":");
        if ((array.length == 3) && sequenceHeader.getTextLength() <= 60) {

            BigText patentId = array[0];
            if(patentId == null || patentId.equals("") || patentId.toString().equalsIgnoreCase("")){
                return SequencePatentFormat.UNKNOWN;
            }

            return SequencePatentFormat.RD;
        }
//        else if (sequenceHeader.split(SEQUENCE_SPLIT2).length > 2) {
//            return SequencePatentFormat.NCBI_1;
//        } else if (PATTERIN_NCBI2.matcher(sequenceHeader.getStringValue()).find()) {
//            // expect to find pn from header like :  //LX252642.1_JP_2016033135-A/14:_HUMAN MONOCLONAL ANTIBODIES TO PROGRAMMED DEATH 1
//            //TODO wrong format: WP011974513.1MULTISPECIES,_hypothetical_protein_[sinorhizobium]yp_001325998.1_hypothetical_protein_smed_0304_[sinorhizobium_medicae_wsm419]abr591
//            Matcher matcher = PATTERIN_NCBI2.matcher(sequenceHeader.getStringValue());
//            return (matcher != null && matcher.find()) ? SequencePatentFormat.NCBI_2 : SequencePatentFormat.UNKNOWN;
//        }
        return SequencePatentFormat.UNKNOWN;
    }


}
