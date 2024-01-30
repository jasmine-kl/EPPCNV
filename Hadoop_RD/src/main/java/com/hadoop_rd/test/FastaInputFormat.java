package com.hadoop_rd.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.LineReader;
import org.seqdoop.hadoop_bam.ReferenceFragment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class FastaInputFormat extends FileInputFormat<Text, ReferenceFragment> {
    private static final Logger logger = LoggerFactory.getLogger(FastaInputFormat.class);
    public static final Charset UTF8 = Charset.forName("UTF8");

    public FastaInputFormat() {
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = super.getSplits(job);
        Collections.sort(splits, new Comparator<InputSplit>() {
            @Override
            public int compare(InputSplit a, InputSplit b) {
                FileSplit fa = (FileSplit)a;
                FileSplit fb = (FileSplit)b;
                return fa.getPath().compareTo(fb.getPath());
            }
        });

        FileSplit fa;
        for(int i = 0; i < splits.size() - 1; ++i) {
            fa = (FileSplit)splits.get(i);
            FileSplit fb = (FileSplit)splits.get(i + 1);
            if (fa.getPath().compareTo(fb.getPath()) != 0) {
                throw new IOException("FastaInputFormat assumes single FASTA input file!");
            }
        }

        List<InputSplit> newSplits = new ArrayList(splits.size());
        fa = (FileSplit)splits.get(0);
        Path path = fa.getPath();
        FileSystem fs = path.getFileSystem(job.getConfiguration());
        FSDataInputStream fis = fs.open(path);
        byte[] buffer = new byte[1024];
        long byte_counter = 0L;
        long prev_chromosome_byte_offset = 0L;
        boolean first_chromosome = true;

        for(int j = 0; j < splits.size(); ++j) {
            FileSplit origsplit = (FileSplit)splits.get(j);

            while(byte_counter < origsplit.getStart() + origsplit.getLength()) {
                long bytes_read = (long)fis.read(byte_counter, buffer, 0, (int)Math.min((long)buffer.length, origsplit.getStart() + origsplit.getLength() - byte_counter));
                if (logger.isDebugEnabled()) {
                    logger.debug("bytes_read: {} of {} splits", bytes_read, splits.size());
                }

                if (bytes_read > 0L) {
                    for(int i = 0; (long)i < bytes_read; ++i) {
                        if (buffer[i] == 62) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("found chromosome at position {}", byte_counter + (long)i);
                            }

                            if (!first_chromosome) {
                                FileSplit fsplit = new FileSplit(path, prev_chromosome_byte_offset, byte_counter + (long)i - 1L - prev_chromosome_byte_offset, origsplit.getLocations());
                                if (logger.isDebugEnabled()) {
                                    logger.debug("adding split: start: {}, length: {}", fsplit.getStart(), fsplit.getLength());
                                }

                                newSplits.add(fsplit);
                            }

                            first_chromosome = false;
                            prev_chromosome_byte_offset = byte_counter + (long)i;
                        }
                    }

                    byte_counter += bytes_read;
                }
            }

            if (j == splits.size() - 1) {
                FileSplit fsplit = new FileSplit(path, prev_chromosome_byte_offset, byte_counter - prev_chromosome_byte_offset, origsplit.getLocations());
                newSplits.add(fsplit);
                if (logger.isDebugEnabled()) {
                    logger.debug("adding split: {}", fsplit);
                }
                break;
            }
        }

        return newSplits;
    }

    @Override
    public boolean isSplitable(JobContext context, Path path) {
        CompressionCodec codec = (new CompressionCodecFactory(context.getConfiguration())).getCodec(path);
        return codec == null;
    }

    @Override
    public RecordReader<Text, ReferenceFragment> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        context.setStatus(genericSplit.toString());
        return new FastaInputFormat.FastaRecordReader(context.getConfiguration(), (FileSplit)genericSplit);
    }

    public static class FastaRecordReader extends RecordReader<Text, ReferenceFragment> {
        private long start;
        private long end;
        private long pos;
        private Path file;
        private int current_split_pos;
        private String current_split_indexseq = null;
        private LineReader lineReader;
        private InputStream inputStream;
        private Text currentKey = new Text();
        private ReferenceFragment currentValue = new ReferenceFragment();
        private Text buffer = new Text();
        public static final int MAX_LINE_LENGTH = 20000;

        public FastaRecordReader(Configuration conf, FileSplit split) throws IOException {
            this.setConf(conf);
            this.file = split.getPath();
            this.start = split.getStart();
            this.end = this.start + split.getLength();
            this.current_split_pos = 0;
            FileSystem fs = this.file.getFileSystem(conf);
            FSDataInputStream fileIn = fs.open(this.file);
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
            CompressionCodec codec = codecFactory.getCodec(this.file);
            if (codec == null) {
                this.positionAtFirstRecord(fileIn);
                this.inputStream = fileIn;
            } else {
                if (this.start != 0L) {
                    throw new RuntimeException("Start position for compressed file is not 0! (found " + this.start + ")");
                }

                this.inputStream = codec.createInputStream(fileIn);
                this.end = 9223372036854775807L;
            }

            this.lineReader = new LineReader(this.inputStream);
        }

        private void positionAtFirstRecord(FSDataInputStream stream) throws IOException {
            if (this.start > 0L) {
                stream.seek(this.start);
            }

            LineReader reader = new LineReader(stream);
            int bytesRead = reader.readLine(this.buffer, (int)Math.min(20000L, this.end - this.start));
            this.current_split_indexseq = this.buffer.toString();
            this.current_split_indexseq = this.current_split_indexseq.substring(1, this.current_split_indexseq.length());
            this.current_split_pos = 0;
            if (FastaInputFormat.logger.isDebugEnabled()) {
                FastaInputFormat.logger.debug("read index sequence: {}", this.current_split_indexseq);
            }

            this.start += (long)bytesRead;
            stream.seek(this.start);
            this.pos = this.start;
        }

        protected void setConf(Configuration conf) {
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        }

        @Override
        public Text getCurrentKey() {
            return this.currentKey;
        }

        @Override
        public ReferenceFragment getCurrentValue() {
            return this.currentValue;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return this.next(this.currentKey, this.currentValue);
        }

        @Override
        public void close() throws IOException {
            this.inputStream.close();
        }

        public Text createKey() {
            return new Text();
        }

        public ReferenceFragment createValue() {
            return new ReferenceFragment();
        }

        public long getPos() {
            return this.pos;
        }

        @Override
        public float getProgress() {
            return this.start == this.end ? 1.0F : Math.min(1.0F, (float)(this.pos - this.start) / (float)(this.end - this.start));
        }

        public String makePositionMessage(long pos) {
            return this.file.toString() + ":" + pos;
        }

        public String makePositionMessage() {
            return this.file.toString() + ":" + this.pos;
        }

        public boolean next(Text key, ReferenceFragment value) throws IOException {
            if (this.pos >= this.end) {
                return false;
            } else {
                int bytesRead = this.lineReader.readLine(this.buffer, 20000);
                this.pos += (long)bytesRead;
                if (bytesRead >= 20000) {
                    throw new RuntimeException("found abnormally large line (length " + bytesRead + ") at " + this.makePositionMessage(this.pos - (long)bytesRead) + ": " + Text.decode(this.buffer.getBytes(), 0, 500));
                } else if (bytesRead <= 0) {
                    return false;
                } else {
                    this.scanFastaLine(this.buffer, key, value);
                    this.current_split_pos += value.getSequence().getLength();
                    return true;
                }
            }
        }

        private void scanFastaLine(Text line, Text key, ReferenceFragment fragment) {
            key.clear();
            key.append(this.current_split_indexseq.getBytes(FastaInputFormat.UTF8), 0, this.current_split_indexseq.getBytes(FastaInputFormat.UTF8).length);
            key.append(Integer.toString(this.current_split_pos).getBytes(FastaInputFormat.UTF8), 0, Integer.toString(this.current_split_pos).getBytes(FastaInputFormat.UTF8).length);
            byte[] bytes = key.getBytes();
            int temporaryEnd = key.getLength();

            for(int i = 0; i < temporaryEnd; ++i) {
                if (bytes[i] == 9) {
                    bytes[i] = 58;
                }
            }

            fragment.clear();
            fragment.setPosition(this.current_split_pos);
            fragment.setIndexSequence(this.current_split_indexseq);
            fragment.getSequence().append(line.getBytes(), 0, line.getBytes().length);
        }
    }
}
