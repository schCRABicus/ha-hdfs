package com.epam.bigdata.training.commons.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FsUtilsTest {

    private static final String FILE_PATH;
    private static final long FILE_SIZE;

    // locate test sample file and calculate its size
    static {
        ClassLoader classLoader = FsUtils.class.getClassLoader();
        File file = new File(classLoader.getResource("sample-test.csv").getFile());

        FILE_PATH = file.getPath();
        FILE_SIZE = file.length();
    }

    @Test
    public void readLineByLineOneLineOnly() {
        // given
        final Configuration hdConf = new Configuration();
        final List<String> lines = new ArrayList<>();

        // when
        FsUtils.readLineByLineWithHeaderAndOffset(hdConf, FILE_PATH, 0, 15, lines::add);

        // then
        Assert.assertEquals(
                Arrays.asList("col-1,col-2,col-3"),
                lines
        );
    }

    @Test
    public void readLineByLineTwoLines() {
        // given
        final Configuration hdConf = new Configuration();
        final List<String> lines = new ArrayList<>();

        // when
        FsUtils.readLineByLineWithHeaderAndOffset(hdConf, FILE_PATH, 0, 20, lines::add);

        // then
        Assert.assertEquals(
                Arrays.asList("col-1,col-2,col-3", "1,2,3"),
                lines
        );
    }

    @Test
    public void readLineByLineThreeLines() {
        // given
        final Configuration hdConf = new Configuration();
        final List<String> lines = new ArrayList<>();

        // when
        FsUtils.readLineByLineWithHeaderAndOffset(hdConf, FILE_PATH, 0, 25, lines::add);

        // then
        Assert.assertEquals(
                Arrays.asList("col-1,col-2,col-3", "1,2,3", "4,,6"),
                lines
        );
    }

    @Test
    public void readLineByLineWholeFile() {
        // given
        final Configuration hdConf = new Configuration();
        final List<String> lines = new ArrayList<>();

        // when
        FsUtils.readLineByLineWithHeaderAndOffset(hdConf, FILE_PATH, 0, FILE_SIZE, lines::add);

        // then
        Assert.assertEquals(
                Arrays.asList("col-1,col-2,col-3", "1,2,3", "4,,6", "7,8,9"),
                lines
        );
    }

    @Test
    public void readLineByLineFromOffset() {
        // given
        final Configuration hdConf = new Configuration();
        final List<String> lines = new ArrayList<>();

        // when
        FsUtils.readLineByLineWithHeaderAndOffset(hdConf, FILE_PATH, 15, 22, lines::add);

        // then
        Assert.assertEquals(
                Arrays.asList("col-1,col-2,col-3", "1,2,3"),
                lines
        );
    }

    @Test
    public void readLineByLineFromOffsetInTheMiddleOfTheLine() {
        // given
        final Configuration hdConf = new Configuration();
        final List<String> lines = new ArrayList<>();

        // when
        FsUtils.readLineByLineWithHeaderAndOffset(hdConf, FILE_PATH, 24, 27, lines::add);

        // then
        Assert.assertEquals(
                Arrays.asList("col-1,col-2,col-3", "4,,6"),
                lines
        );
    }

}