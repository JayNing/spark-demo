package com.ning.hadoop.customrecordreader.parsencbi.custom;

/**
 * @author Liu Jianwei
 * @date 2018/6/1
 */
public class PatentFastaInputFormat extends FastaFileInputFormat {

    @Override
    public boolean shouldIgnore(String fileName) {
        return !fileName.contains(".patent.");
    }
}
