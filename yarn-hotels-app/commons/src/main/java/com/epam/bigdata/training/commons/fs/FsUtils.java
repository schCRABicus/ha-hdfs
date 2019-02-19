package com.epam.bigdata.training.commons.fs;

import com.epam.bigdata.training.commons.tracer.HTracerUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.function.Consumer;

/**
 * Dedicated class responsible for file system level operations.
 */
public class FsUtils {

    private static final Logger log = LoggerFactory.getLogger(FsUtils.class);

    /**
     * Read the source contents line by line and output to the provided sink.
     * @param configuration File System configuration.
     * @param source        Source to read the data from.
     * @param sink          Output sink.
     * @throws FsException if fails to get the file system or fails to read the input source.
     */
    public static void readLineByLine(Configuration configuration, String source, Consumer<String> sink) throws FsException {
        log.info("Going to connect and read from {}", source);

        Path path = new Path(source);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);

            FileSystem finalFs = fs;
            HTracerUtils.trace("File System", "FS read line by line", configuration, () -> {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(finalFs.open(path)))) {
                    String line = br.readLine();

                    while (line != null){
                        sink.accept(line);

                        // be sure to read the next line otherwise you'll get an infinite loop
                        line = br.readLine();
                    }
                } catch (Exception e) {
                    log.warn("Failed to read the contents of {}", source, e);
                    throw new FsException("Failed to read the contents of the provided source", e);
                }
            });

        } catch (IOException e) {
            log.warn("Failed to get fs", e);
            throw new FsException("Failed to get file system", e);
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                log.warn("Failed to close File System : ", e);
            }
        }
    }
    /**
     * Read the source contents line by line and output to the provided sink.
     * @param configuration File System configuration.
     * @param source        Source to read the data from.
     * @param sink          Output sink.
     * @throws FsException if fails to get the file system or fails to read the input source.
     */
    public static void readLineByLineWithHeaderAndOffset(Configuration configuration, String source, long start, long end, Consumer<String> sink) throws FsException {
        log.info("Going to connect and read from {}", source);

        Path path = new Path(source);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);

            FileSystem finalFs = fs;
            HTracerUtils.trace("File System", "FS read line by line with header and offsets", configuration, () -> {
                // read the header first
                final String header = readFirstLine(finalFs, path);
                sink.accept(header);

                // now, seek to the specified offset and read until the specified limit
                try (FSDataInputStream fsDataInputStream = finalFs.open(path);
                     BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream))) {
                    long offset = Math.max(start, header.length() + 1);
                    fsDataInputStream.seek(offset);

                    String line = br.readLine();

                    // current position in input stream. Can't use inputStream.getPos() as BufferReader buffers more then is actually processed
                    long current = offset;

                    while (line != null && current <= end) {
                        sink.accept(line);

                        current += line.length() + 1; //one byte for '\n'

                        // be sure to read the next line otherwise you'll get an infinite loop
                        line = br.readLine();
                    }
                } catch (Exception e) {
                    log.warn("Failed to read the contents of {}", source, e);
                    throw new FsException("Failed to read the contents of the provided source", e);
                }
            });

        } catch (IOException e) {
            log.warn("Failed to get fs", e);
            throw new FsException("Failed to get file system", e);
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                log.warn("Failed to close File System : ", e);
            }
        }
    }

    private static String readFirstLine(FileSystem fs, Path path) {
        try (FSDataInputStream fsDataInputStream = fs.open(path);
             BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream))) {

            return br.readLine();
        } catch (Exception e) {
            log.warn("Failed to read the first line of {}", path, e);
            throw new FsException("Failed to read the first line of the provided source", e);
        }
    }

    /**
     * Writes out the specified lines to the specified target.
     * @param configuration
     * @param target
     * @param lines
     * @throws FsException
     */
    public static void write(Configuration configuration, String target, List<String> lines) throws FsException {
        Path file = new Path(target);

        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);

            FileSystem finalFs = fs;
            HTracerUtils.trace("File System", "FS write", configuration, () -> {
                try {
                    if (finalFs.exists(file)) {
                        finalFs.delete(file, true);
                    }
                } catch (Exception e) {
                    throw new FsException("Failed to delete alerady existing file", e);
                }

                try (BufferedWriter br = new BufferedWriter( new OutputStreamWriter( finalFs.create( file ), "UTF-8" ) )) {
                    for (String line : lines) {
                        br.write(line);
                        br.newLine();
                    }
                } catch (Exception e) {
                    log.error("Failed to write the lines into hdfs path {}", target, e);
                    throw new FsException("Failed to write the lines into hdfs path " + target, e);
                }
            });

        } catch (IOException e) {
            log.warn("Failed to get fs", e);
            throw new FsException("Failed to get file system", e);
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                log.warn("Failed to close File System : ", e);
            }
        }
    }
}
