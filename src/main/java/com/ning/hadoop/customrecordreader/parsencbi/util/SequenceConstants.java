package com.ning.hadoop.customrecordreader.parsencbi.util;

/**
 * @author: Arthur Hu
 * @date: 2018/1/5 下午4:17
 * Description:
 */
public class SequenceConstants {

    public static final String SEQUENCE_TYPE_CONF = "lifescience.hadoop.sequence_type";

    public static final String SEQUENCE_PATENT_STATUS = "lifescience.hadoop.sequence_patent_status";

    public static final String SEQUENCE_INDEX_TYPE = "lifescience.hadoop.sequence_es_index_type";

    public static final String SEQUENCE_INDEX_SERVER = "lifescience.hadoop.sequence_es_index_server";

    public static final String COMMA_CHAR = ".";

    public static final String COLON_CHAR = ":";

    public static final String WAVY_LINE_CHAR = "~";
    public static final String SPRIT_CHAR = "/";

    public static final String NO_PATENT_HEADER = "NO_PATENT";

    public static final String LINE_PATTERN = "\r|\n";

    public static final String LINE_BREAKER = "\n";

    public static final String PN_PATTERN = "^[A-Za-z]{2}[\\d]{4,}[A-Za-z]{0,1}[\\d]{0,1}$";

    public static final String HEADER_VALUE_SPLITER = "#HV#";

    public static final String HEADER_HEADER_SPLITOR = "#H#";

    public static final String HEADER_HASH_ID_SPLITOR = "#HI#";

    public static final String FASTA_START_CHAR = ">";

    public static final String UNDERLINE_CHAR = "_";

    public static final int MAX_ARRAY_LENGTH_TWO = 2;

    public static final int DEFAULT_MAX_RECORD_LIMIT = 1 * 1024 * 1024;//1MB

    public static final int WRITE_BUFFER_SIZE = 5 * 1024 * 1024;//1MB


    public static class SequencePatentStatus {
        public static String PATENT = "patent";
        public static String NON_PATENT = "non_patent";
        public static String BOTH = "both";
    }

    public static class IndexType {
        public static final String META = "meta";
        public static final String HASH = "hash";
    }

}
