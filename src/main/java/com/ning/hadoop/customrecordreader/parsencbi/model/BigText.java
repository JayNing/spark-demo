package com.ning.hadoop.customrecordreader.parsencbi.model;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * BigText is a expand of String Class. It can indicates and operate large text.
 * <p>
 * It implements most of methods of String and StringBuffer.
 * <p>
 * BigText can be used in Hadoop as a Mapreduce data type. Because it implements the WritableComparable interface.
 *
 * @author: Arthur Hu
 * @date: 2018/1/26 下午4:27
 * Description:
 */
public class BigText implements WritableComparable<BigText>, java.io.Serializable, Comparable<BigText> {

    private StringBuffer text;

    private int textLength;


    public BigText() {
        this.text = new StringBuffer();
        this.textLength = 0;
    }

    public BigText(int blockLimit) {
        this();
    }

    public BigText(Text value) {
        this();
        String valueStr = value.toString();
        this.append(valueStr);
    }

    public BigText(String value) {
        this();
        this.append(value);
    }

    public BigText(String value, int blockLimit) {
        this.append(value);
    }

    public BigText(BigText oldBigText) {
        this.text = oldBigText.text;
    }


    public BigText(List<String> text, int blockcount, int textLength, int blockLimit) {
        for (String s : text) {
            this.text.append(s);
        }
        this.textLength = textLength;
    }


    public boolean isEmpty() {
        return StringUtils.isEmpty(this.text.toString());
    }

    /**
     * clear all data
     */
    public void clear() {
        this.textLength = 0;
        this.text.setLength(0);
    }

    public boolean startsWith(String prefix) {
        return this.text.toString().startsWith(prefix);
    }


    public boolean endsWith(String suffix) {
        return this.text.toString().endsWith(suffix);

    }


    public int firstIndexOf(String ch) {
        return this.text.indexOf(ch);
    }

    public int lastIndexOf(String ch) {
        return this.text.lastIndexOf(ch);
    }


    public BigText substring(long beginIndex, long endIndex) {
        if (endIndex > this.text.length() || beginIndex < 0) {
            throw new IllegalArgumentException("the index out of range of bigtext.");
        }

        String string = beginIndex >= endIndex ? this.text.substring((int) beginIndex) : this.text.substring((int) beginIndex, (int) endIndex);
        BigText bigText = new BigText();
        bigText.append(string);
        return bigText;
    }


    public BigText substring(long beginIndex) throws Exception {
        if (beginIndex < 0) {
            throw new Exception("msg : " + beginIndex);
        }
//
//        long endIndex = textLength;
//
//        return ((beginIndex == 0) && (endIndex == textLength)) ? this
//                : substring(beginIndex, endIndex);
        String result = this.text.substring((int) beginIndex);
        return new BigText(result);
    }


    @Override
    public int compareTo(BigText o) {

        if (o == null) {
            return 1;
        }

        int objectTextLength = o.getTextLength();

        if (this.textLength > objectTextLength) {
            return 1;
        } else if (this.textLength == objectTextLength) {

            for (int i = 0; i < this.textLength; i++) {
                char thisChar = getStringValue().charAt(i);
                char objectChar = o.getStringValue().charAt(i);

                if (thisChar > objectChar) {
                    return 1;
                } else if (thisChar < objectChar) {
                    return -1;
                }
            }
        } else {
            return -1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        WritableUtils.writeVInt(out, this.textLength);
        WritableUtils.writeString(out, this.text.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        int length = WritableUtils.readVInt(in);
        this.textLength = length;

        String inputString = WritableUtils.readString(in);

        this.text = new StringBuffer(inputString);

    }

    private class CharLocationPair {

        private int blockIndex;
        private int rangeIndex;

        public CharLocationPair() {
            this.blockIndex = 0;
            this.rangeIndex = 0;
        }

        public CharLocationPair(int block, int rangeIndex) {
            this.blockIndex = block;
            this.rangeIndex = rangeIndex;
        }

        public int getBlockIndex() {
            return blockIndex;
        }

        public void setBlockIndex(int blockIndex) {
            this.blockIndex = blockIndex;
        }

        public int getRangeIndex() {
            return rangeIndex;
        }

        public void setRangeIndex(int rangeIndex) {
            this.rangeIndex = rangeIndex;
        }
    }


    public BigText append(Text value) {
        if (value == null || value.getLength() <= 0) {
            return this;
        }

        String valueStr = value.toString();

        return append(valueStr);
    }

    public BigText append(BigText bigText) {
        this.text.append(bigText.text);
        this.textLength += bigText.getTextLength();
        return this;
    }

    public BigText append(String block) {
        this.text.append(block);
        this.textLength += block.length();
        return this;
    }


    public boolean contains(CharSequence s) {
        return firstIndexOf(s.toString()) > -1;
    }


    /**
     * Replace all substring matched regex to replacement.
     * this method return a new BigText instance
     *
     * @param regex
     * @param replacement
     * @return
     */
    public BigText replaceAll(String regex, String replacement) {
        String textString = this.text.toString();

        String result = textString.replaceAll(regex, replacement);

        return new BigText(result);
    }


    public BigText replaceAll(String regex, String replacement, int blockLimit) {

        BigText replacedBigText = new BigText();

        String str = this.text.toString();
        String newStr = str.replaceAll(regex, replacement);
        replacedBigText.append(newStr);
        return replacedBigText;
    }

    public BigText replace(String regex, String replacement) {
        BigText replacedBigText = new BigText();
        String str = this.text.toString();
        str = str.replace(regex, replacement);
        replacedBigText.append(str);
        return replacedBigText;
    }


    public BigText toLowerCase() {
        BigText lowerCase = new BigText();
        String lowerStr = this.text.toString().toLowerCase();
        lowerCase.append(lowerStr);

        return lowerCase;

    }

    public BigText toUpperCase() {
        BigText upperCase = new BigText();
        String upperStr = this.text.toString().toUpperCase();
        upperCase.append(upperStr);

        return upperCase;

    }

    public BigText[] split(String regx) {
        String[] array = this.text.toString().split(regx);
        List<BigText> bigTexts = new ArrayList<>();
        for (String s : array) {
            BigText bigText = new BigText(s);
            bigTexts.add(bigText);
        }
        return bigTexts.toArray(new BigText[bigTexts.size()]);
//        if (StringUtils.isEmpty(regx)) {
//            return new BigText[]{this};
//        }
//
//        int size = 0;
//        int regrexSize = regx.length();
//
//        List<BigText> bigTextList = new ArrayList<>();
//        BigText temple = this;
//        int index = -1;
//        while ((index = temple.firstIndexOf(regx)) > -1) {
//            BigText front = temple.substring(0, index);
//            bigTextList.add(front);
//            temple = temple.substring(index + regrexSize);
//            size++;
//        }
//
//        bigTextList.add(temple);
//
//        return bigTextList.toArray(new BigText[size + 1]);

    }

    public int getTextLength() {
        return this.text.length();
    }

    public List<String> getText() {
        List<String> list = new ArrayList<>();
        list.add(this.text.toString());
        return list;
    }

    public String getStringValue() {
        return this.text.toString();
    }

    @Override
    public String toString() {
        return this.text.toString();
    }


    @Override
    public int hashCode() {
        int result = 17;

        result = 31 * result + (text == null ? 0 : this.text.toString().hashCode());
        result = 31 * result + (textLength ^ (textLength >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof BigText) {
            BigText bigText = (BigText) o;
            String textValue = getStringValue();

            String otherText = bigText.getStringValue();

            return textValue.equals(otherText) && this.textLength == bigText.getTextLength();
        }

        return false;


    }

    public void setText(StringBuffer text) {
        this.text = text;
    }
}
