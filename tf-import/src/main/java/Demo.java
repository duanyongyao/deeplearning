import com.google.common.collect.Lists;
import tools.SourceLoader;

import java.util.List;

/**
 * @author yongyaoduan
 * @create 2018-10-19 20:00
 * @desc 用数据实际调用一下服务函数
 **/
public class Demo {
    public static void main(String[] args) {
        //拿到示例数据
        List<String> stringList = SourceLoader.load("data/server_samples.csv");

        //转换一下数据类型
        List<List<Float>> serverSamples = Lists.newArrayList();
        List<Integer> labels = Lists.newArrayList();//用于存放数据的label
        List<Integer> plabels = Lists.newArrayList();//用于存放预测的label

        for (String eachString : stringList) {
            String[] cols = eachString.split(",");//第一列是label，后面是向量
            labels.add(Integer.parseInt(cols[0]));
            List<Float> feature = Lists.newArrayList();
            for (int i = 1; i < cols.length; i++) {
                feature.add(Float.parseFloat(cols[i]));
            }

            serverSamples.add(feature);
        }

        //模拟udf的调用过程
        TFServerUDF theUDF = new TFServerUDF();
        long start = System.currentTimeMillis();
        for (List<Float> eachSample : serverSamples) {
            Integer plabel = theUDF.evaluate(eachSample) > 0.5 ? 1 : 0;
            plabels.add(plabel);
        }

        long end = System.currentTimeMillis();

        //计算一下单机耗时  正常应该在10秒内跑完这一万样本
        System.out.println("测试样本总量:" + serverSamples.size());
        System.out.println("一共耗时:" + (end - start) + "毫秒");

        //计算一下准确率  应为83.35%
        int rightNum = 0;
        for (int i = 0; i < labels.size(); i++) {
            if (labels.get(i).equals(plabels.get(i))) {
                rightNum += 1;
            }
        }
        System.out.println("准确率:" + String.format("%.2f",100D*rightNum/serverSamples.size())+"%");
    }
}
