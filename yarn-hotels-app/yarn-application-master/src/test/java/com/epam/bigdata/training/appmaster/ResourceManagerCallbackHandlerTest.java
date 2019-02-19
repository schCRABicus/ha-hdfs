package com.epam.bigdata.training.appmaster;

import java.io.File;

public class ResourceManagerCallbackHandlerTest {

    private static final String FILE_PATH;
    private static final long FILE_SIZE;

    // locate test sample file and calculate its size
    static {
        ClassLoader classLoader = ResourceManagerCallbackHandlerTest.class.getClassLoader();
        File file = new File(classLoader.getResource("sample-test.csv").getFile());

        FILE_PATH = file.getPath();
        FILE_SIZE = file.length();
    }

//    @Test
//    public void calculateSplitBoundariesForSingleContainer() throws Exception {
//        // given
//        final Configuration hdConf = new Configuration();
//        final LaunchConfiguration lConf = new LaunchConfiguration();
//
//        lConf.setAppInputPath(FILE_PATH);
//        lConf.setNumTotalContainers(1);
//
//        FileSystem fs = FileSystem.get(hdConf);
//
//        // when
//        List<SplitBoundary> result = ResourceManagerCallbackHandler.calculateSplitBoundaries(fs, lConf);
//
//        // then
//        Assert.assertEquals(
//                Collections.singletonList(new SplitBoundary(0, FILE_SIZE)),
//                result
//        );
//    }
//
//    @Test
//    public void calculateSplitBoundariesForTwoContainerSplitsOnLineEnds() throws Exception {
//        // given
//        final Configuration hdConf = new Configuration();
//        final LaunchConfiguration lConf = new LaunchConfiguration();
//
//        lConf.setAppInputPath(FILE_PATH);
//        lConf.setNumTotalContainers(2);
//
//        FileSystem fs = FileSystem.get(hdConf);
//
//        // when
//        List<SplitBoundary> result = ResourceManagerCallbackHandler.calculateSplitBoundaries(fs, lConf);
//
//        // then
//        Assert.assertEquals(
//                Arrays.asList(new SplitBoundary(0, 15), new SplitBoundary(16, 34)),
//                result
//        );
//    }
}
