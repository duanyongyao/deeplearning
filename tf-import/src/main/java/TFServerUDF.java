import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.nd4j.autodiff.samediff.SDVariable;
import org.nd4j.autodiff.samediff.SameDiff;
import org.nd4j.imports.graphmapper.tf.TFGraphMapper;
import org.nd4j.linalg.factory.Nd4j;
import org.tensorflow.framework.GraphDef;
import tools.SourceLoader;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * @author yongyaoduan
 * @create 2018-10-19 19:33
 * @desc 用于tensorflow服务的udf
 **/
public class TFServerUDF extends UDF {
    private static TFGraphMapper tfUtil = TFGraphMapper.getInstance();//tf计算图转换工具
    private static byte[] bGraph = SourceLoader.loadHDFSTFModel("model/dnn.pb");//读取好的二进制计算图
    private static GraphDef tfGraph = null;


    static {
        try {
            tfGraph = tfUtil.parseGraphFrom(bGraph);//解析计算图
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 保证了原子性的udf方法
     *
     * @param featureList 由于hive天然支持list，这里用list作为输入，实际的udf可以输入n个特征，完成向量化后调用模型
     * @return
     * @throws HiveException
     */
    public Float evaluate(List<Float> featureList) {

        SameDiff graph = tfUtil.importGraph(tfGraph);
        SDVariable inTensor = graph.variableMap().get("input");//输入张量
        SDVariable outTensor = graph.getVariable("prediction");//输出张量

        float[] feature = new float[featureList.size()];
        for (int i = 0; i < featureList.size(); i++) {
            feature[i] = featureList.get(i);
        }

        //往tensor里面写值
        inTensor.setArray(Nd4j.create(feature).reshape(-1, 1));

        //拿到输出值
        Float outputOri = outTensor.eval().getFloat(0);
        return outputOri;
    }
}
