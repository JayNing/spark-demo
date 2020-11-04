package com.ning.hadoop.customrecordreader.customv2;

import java.io.Serializable;
import java.util.List;

/**
 * ClassName: NcbiParseBean
 * Description:
 * date: 2020/11/3 10:27
 *
 * @author ningjianjian
 */
public class NcbiParseBean implements Serializable {
    private String seq_val;
    private List<String> gi_list;

    public String getSeq_val() {
        return seq_val;
    }

    public void setSeq_val(String seq_val) {
        this.seq_val = seq_val;
    }

    public List<String> getGi_list() {
        return gi_list;
    }

    public void setGi_list(List<String> gi_list) {
        this.gi_list = gi_list;
    }
}
