package com.ning.hadoop.customrecordreader.parsencbi.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author: Arthur Hu
 * @date: 2018/1/4 上午1:13
 * Description:
 */
public class FastaLineReader extends LineReader {

    private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private InputStream in;
    private byte[] buffer;
    // the number of bytes of real data in the buffer
    private int bufferLength = 0;
    // the current position in the buffer
    private int bufferPosn = 0;

    private static final byte DELIMITOR = '>';

    // The line delimiter
    private final byte[] recordDelimiterBytes;

    private static final Logger logger = LoggerFactory.getLogger(FastaLineReader.class);


    /**
     * Create a line reader that reads from the given stream using the
     * default buffer-size (64k).
     * @param in The input stream
     * @throws IOException
     */
    public FastaLineReader(InputStream in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Create a line reader that reads from the given stream using the
     * given buffer-size.
     * @param in The input stream
     * @param bufferSize Size of the read buffer
     * @throws IOException
     */
    public FastaLineReader(InputStream in, int bufferSize) {
        super(in,bufferSize);
        this.in = in;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = null;
    }

    /**
     * Create a line reader that reads from the given stream using the
     * <code>io.file.buffer.size</code> specified in the given
     * <code>Configuration</code>.
     * @param in input stream
     * @param conf configuration
     * @throws IOException
     */
    public FastaLineReader(InputStream in, Configuration conf) throws IOException {
        this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
    }

    /**
     * Create a line reader that reads from the given stream using the
     * default buffer-size, and using a custom delimiter of array of
     * bytes.
     * @param in The input stream
     * @param recordDelimiterBytes The delimiter
     */
    public FastaLineReader(InputStream in, byte[] recordDelimiterBytes) {
        super(in,recordDelimiterBytes);
        this.in = in;
        this.bufferSize = DEFAULT_BUFFER_SIZE;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    /**
     * Create a line reader that reads from the given stream using the
     * given buffer-size, and using a custom delimiter of array of
     * bytes.
     * @param in The input stream
     * @param bufferSize Size of the read buffer
     * @param recordDelimiterBytes The delimiter
     * @throws IOException
     */
    public FastaLineReader(InputStream in, int bufferSize,
                      byte[] recordDelimiterBytes) {
        super(in,bufferSize,recordDelimiterBytes);
        this.in = in;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    /**
     * Create a line reader that reads from the given stream using the
     * <code>io.file.buffer.size</code> specified in the given
     * <code>Configuration</code>, and using a custom delimiter of array of
     * bytes.
     * @param in input stream
     * @param conf configuration
     * @param recordDelimiterBytes The delimiter
     * @throws IOException
     */
    public FastaLineReader(InputStream in, Configuration conf,
                      byte[] recordDelimiterBytes) throws IOException {
        super(in,conf,recordDelimiterBytes);
        this.in = in;
        this.bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }


    @Override
    public int readLine(Text str, int maxLineLength,
                        int maxBytesToConsume) throws IOException {
        str.clear();
        boolean endDelimitor = false;
        boolean startDelimitor = false;
        boolean endOfFile = false;
        long bytesConsumed = 0;

        try{
            do{


                int startPosn = bufferPosn;
                if (bufferPosn >= bufferLength) {
                    startPosn = bufferPosn = 0;
                    bufferLength = in.read(buffer);
                    if (bufferLength <= 0) {
                        endOfFile=true;
                        break; // EOF
                    }
                }

                // search '>' in new line
                for(;bufferPosn<bufferLength;++bufferPosn){
                    if(buffer[bufferPosn] == DELIMITOR){
                        if(!startDelimitor){
                            startDelimitor=true;
                            startPosn = bufferPosn;
                            continue;
                        }

                        if(startDelimitor && !endDelimitor){
                            int prevLPosn = bufferPosn-1;
                            if(prevLPosn>=0 && buffer[prevLPosn] != '\n' && buffer[prevLPosn] != '\r'){
                                continue;
                            }
                            endDelimitor=true;
                            break;
                        }

                    }


                }

                int readLength = bufferPosn - startPosn;
                bytesConsumed += readLength;
                if(readLength >0){
                    str.append(buffer,startPosn,readLength);
                }

            }while (!endDelimitor);
        }catch (Exception e){
            logger.error("readLine@FastaLineReader, read fasta unit error.",e);
            e.printStackTrace();
        }
        if (bytesConsumed >= Integer.MAX_VALUE) {
            throw new IOException("Too many bytes before newline: " + bytesConsumed+", pos="+this.getBufferPosn());
        }
        return (int)bytesConsumed;

    }

    @Override
    protected int getBufferPosn() {
        return this.bufferPosn;
    }

}
