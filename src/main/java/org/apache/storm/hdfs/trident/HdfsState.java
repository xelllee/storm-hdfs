package org.apache.storm.hdfs.trident;

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.format.SequenceFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;

import org.apache.storm.hdfs.trident.sync.SyncPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HdfsState implements State {

    public static abstract class Options implements Serializable {

        protected String fsUrl;
        protected FileSystem fs;
        private Path currentFile;
        protected FileRotationPolicy rotationPolicy;
        protected FileNameFormat fileNameFormat;
        protected int rotation = 0;
        protected Configuration hdfsConfig;
        protected ArrayList<RotationAction> rotationActions = new ArrayList<RotationAction>();
        protected SyncPolicy syncPolicy = null;

        abstract void closeOutputFile() throws IOException;

        abstract Path createOutputFile() throws IOException;

        abstract void execute(TridentTuple tuple) throws IOException;

        abstract void doPrepare(Map conf, int partitionIndex, int numPartitions) throws IOException;

        protected void rotateOutputFile() throws IOException {
            LOG.info("Rotating output file...");
            long start = System.currentTimeMillis();
            closeOutputFile();
            this.rotation++;

            Path newFile = createOutputFile();
            LOG.info("Performing {} file rotation actions.", this.rotationActions.size());
            for (RotationAction action : this.rotationActions) {
                action.execute(this.fs, this.currentFile);
            }
            this.currentFile = newFile;
            long time = System.currentTimeMillis() - start;
            LOG.info("File rotation took {} ms.", time);


        }

        void prepare(Map conf, int partitionIndex, int numPartitions) {
            if (this.rotationPolicy == null) throw new IllegalStateException("RotationPolicy must be specified.");
            if (this.fsUrl == null) {
                throw new IllegalStateException("File system URL must be specified.");
            }
            this.fileNameFormat.prepare(conf, partitionIndex, numPartitions);
            this.hdfsConfig = new Configuration();
            try {
                doPrepare(conf, partitionIndex, numPartitions);
                this.currentFile = createOutputFile();

            } catch (Exception e) {
                throw new RuntimeException("Error preparing HdfsState: " + e.getMessage(), e);
            }
        }

    }

    public static class HdfsFileOptions extends Options {

        private FSDataOutputStream out;
        protected RecordFormat format;
        private long offset = 0;
        private String compressionCodec = "default";
        private CompressionCodec codec;
        private CompressionOutputStream compressionOut;
        private transient CompressionCodecFactory codecFactory;
        private boolean compressed = false;


        public HdfsFileOptions withFsUrl(String fsUrl) {
            this.fsUrl = fsUrl;
            return this;
        }

        public HdfsFileOptions withFileNameFormat(FileNameFormat fileNameFormat) {
            this.fileNameFormat = fileNameFormat;
            return this;
        }

        public HdfsFileOptions withRecordFormat(RecordFormat format) {
            this.format = format;
            return this;
        }

        public HdfsFileOptions withRotationPolicy(FileRotationPolicy rotationPolicy) {
            this.rotationPolicy = rotationPolicy;
            return this;
        }

        public HdfsFileOptions withSyncPolicy(SyncPolicy syncPolicy) {
            this.syncPolicy = syncPolicy;
            return this;
        }

        public HdfsFileOptions withCompressionCodec(String compressionCodec) {
            this.compressionCodec = compressionCodec;
            this.compressed = true;
            return this;
        }

        public HdfsFileOptions addRotationAction(RotationAction action) {
            this.rotationActions.add(action);
            return this;
        }

        @Override
        void doPrepare(Map conf, int partitionIndex, int numPartitions) throws IOException {
            LOG.info("Preparing HDFS Bolt...");
            this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
            this.codecFactory = new CompressionCodecFactory(hdfsConfig);
            this.codec = this.codecFactory.getCodecByName(this.compressionCodec);
        }

        @Override
        void closeOutputFile() throws IOException {

            if (compressed) {
                this.compressionOut.finish();
                this.out.hsync();

                for (int i = 0; i < 5; i++) {
                    try {
                        this.compressionOut.close();
                        LOG.info("Compressed file closed, tried  " + (i + 1) + " times.");
                        break;
                    } catch (IOException ie) {
                        LOG.info("Can not close compressionOutput for " + i + " times + waiting 200ms .");
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException ire) {
                            ire.printStackTrace();
                        }
                    }
                }
            } else {
                this.out.hsync();
                this.out.close();
            }
        }

        @Override
        Path createOutputFile() throws IOException {
            Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
            this.out = this.fs.create(path);

            if (compressed) {
                compressionOut = codec.createOutputStream(out);
            }

            return path;
        }

        @Override
        public void execute(TridentTuple tuple) throws IOException {
            byte[] bytes = this.format.format(tuple);

            if (compressed) {
                compressionOut.write(bytes);
            } else {
                out.write(bytes);
            }

            this.offset += bytes.length;

            if (this.syncPolicy != null && this.syncPolicy.mark(tuple, this.offset)) {
                if (compressed) {
                    this.compressionOut.finish();
                }
                this.out.hsync();
                this.syncPolicy.reset();
            }

            if (this.rotationPolicy.mark(tuple, this.offset)) {
                rotateOutputFile();
                this.offset = 0;
                this.rotationPolicy.reset();
            }
        }
    }

    public static class SequenceFileOptions extends Options {
        private SequenceFormat format;
        private SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.BLOCK;
        private SequenceFile.Writer writer;
        private String compressionCodec = "default";
        private transient CompressionCodecFactory codecFactory;
        protected SyncPolicy syncPolicy;


        public SequenceFileOptions withCompressionCodec(String codec) {
            this.compressionCodec = codec;
            return this;
        }

        public SequenceFileOptions withFsUrl(String fsUrl) {
            this.fsUrl = fsUrl;
            return this;
        }

        public SequenceFileOptions withFileNameFormat(FileNameFormat fileNameFormat) {
            this.fileNameFormat = fileNameFormat;
            return this;
        }

        public SequenceFileOptions withSequenceFormat(SequenceFormat format) {
            this.format = format;
            return this;
        }

        public SequenceFileOptions withRotationPolicy(FileRotationPolicy rotationPolicy) {
            this.rotationPolicy = rotationPolicy;
            return this;
        }

        public SequenceFileOptions withCompressionType(SequenceFile.CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public SequenceFileOptions addRotationAction(RotationAction action) {
            this.rotationActions.add(action);
            return this;
        }

        public SequenceFileOptions withSyncPolicy(SyncPolicy syncPolicy) {
            this.syncPolicy = syncPolicy;
            return this;
        }

        @Override
        void doPrepare(Map conf, int partitionIndex, int numPartitions) throws IOException {
            LOG.info("Preparing Sequence File State...");
            if (this.format == null) throw new IllegalStateException("SequenceFormat must be specified.");

            this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
            this.codecFactory = new CompressionCodecFactory(hdfsConfig);
        }

        @Override
        Path createOutputFile() throws IOException {
            Path p = new Path(this.fsUrl + this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
            this.writer = SequenceFile.createWriter(
                    this.hdfsConfig,
                    SequenceFile.Writer.file(p),
                    SequenceFile.Writer.keyClass(this.format.keyClass()),
                    SequenceFile.Writer.valueClass(this.format.valueClass()),
                    SequenceFile.Writer.compression(this.compressionType, this.codecFactory.getCodecByName(this.compressionCodec))
            );
            return p;
        }

        @Override
        void closeOutputFile() throws IOException {
            this.writer.hsync();

            this.writer.close();
        }

        @Override
        public void execute(TridentTuple tuple) {
            try {
                this.writer.append(this.format.key(tuple), this.format.value(tuple));
                long offset = this.writer.getLength();

                if (this.syncPolicy.mark(tuple, offset)) {
                    this.writer.hsync();
                    this.syncPolicy.reset();
                }

                if (this.rotationPolicy.mark(tuple, offset)) {
                    rotateOutputFile();
                    this.rotationPolicy.reset();
                }
            } catch (IOException e) {
                LOG.warn("write/sync failed. Triggering batch replay...", e);
                throw new FailedException(e);
            }

        }

    }

    public static final Logger LOG = LoggerFactory.getLogger(HdfsState.class);
    private Options options;

    HdfsState(Options options) {
        this.options = options;
    }

    void prepare(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        this.options.prepare(conf, partitionIndex, numPartitions);
    }

    @Override
    public void beginCommit(Long txId) {
    }

    @Override
    public void commit(Long txId) {
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector tridentCollector) {
        try {
            for (TridentTuple tuple : tuples) {
                this.options.execute(tuple);
            }
        } catch (IOException e) {
            LOG.warn("Failing batch due to IOException.", e);
            throw new FailedException(e);
        }
    }
}
