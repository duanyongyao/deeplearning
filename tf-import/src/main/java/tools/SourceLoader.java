package tools;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/***
 * 作用:文件读取工具
 * 作者:段永耀
 * 日期:2018-06-22
 */
public class SourceLoader {
    private static Logger log = LoggerFactory.getLogger(SourceLoader.class);

    /**
     * 将文件内容加载到List
     *
     * @param fileName
     * @return
     */
    public static List<String> load(String fileName) {
        List<String> outputList = new ArrayList<String>();
        try {
            File file = new File(fileName);
            if (!file.exists()) {
                log.error("FileNotFoound:{}", fileName);
                return new ArrayList<String>();
            }
            BufferedReader br = Files.newBufferedReader(Paths.get(file.getAbsolutePath()), Charset.forName("UTF-8"));
            String line = null;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                outputList.add(line);
            }

            br.close();
        } catch (IOException e) {
            log.error("FileLoadError:{},reason:{}", fileName, e.getMessage());
        }

        log.info("FileLoadDone:{}", fileName);
        return outputList;
    }

    /**
     * 按行读取hdfs的文件内容
     *
     * @param fileName
     * @return
     */
    public static List<String> loadHDFS(String fileName) {
        List<String> outputList = new ArrayList<String>();
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(fileName), conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(fileName));

            String line = null;
            while ((line = hdfsInStream.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                outputList.add(line);
            }
            hdfsInStream.close();
            fs.close();
        } catch (IOException e) {
            log.error("FileLoadError:{},reason:{}", fileName, e.getMessage());
            e.printStackTrace();
        }

        log.info("FileLoadDone:{}", fileName);
        return outputList;
    }

    /**
     * 从对应路径读取pb模型，也可用于读取本地模型
     * @param modelPath
     * @return
     */
    public static byte[] loadHDFSTFModel(String modelPath) {
        byte[] bGraph = null;
        try {
            Configuration conf = new Configuration();
            URI uri = URI.create(modelPath);
            FileSystem fs = FileSystem.get(uri, conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(modelPath));
            bGraph = IOUtils.toByteArray(hdfsInStream);
            hdfsInStream.close();
            fs.close();
            conf.clear();
        } catch (IOException e) {
            log.error("FileLoadError:{},reason:{}", modelPath, e.getMessage());
            e.printStackTrace();
        }

        log.info("FileLoadDone:{}", modelPath);
        return bGraph;
    }
}
