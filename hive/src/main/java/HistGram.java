import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yongyaoduan
 * @create 2018-10-25 19:57
 * @desc 等距直方图
 **/
public class HistGram extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        //1. 检查传入参数类型(其实也可以不检查，只是便于找问题，逻辑上也严格一些而已)
        if (parameters.length != 3) {//检查输入参数的个数
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly 3 argument is expected.");
        }

        //根据参数的类型 判断传参是否正确
        ObjectInspector oi1 = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);

        //第一个参数是double类型，那么首先需要时基本数据类型(与List、Map、Struct想反)
        if (oi1.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Argument must be PRIMITIVE, but "
                            + oi1.getCategory().name()
                            + " was passed.");
        }

        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi1;

        //然后判断是基本数据类型里面的double
        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
            throw new UDFArgumentTypeException(0,
                    "Argument 1 must be DOUBLE, but "
                            + inputOI.getPrimitiveCategory().name()
                            + " was passed.");
        }

        //只有这一句是必须的
        return new HistGramEvaluator();
    }

    public static class HistGramEvaluator extends GenericUDAFEvaluator {

        PrimitiveObjectInspector colValueOI, stepSizeOI, histNumOI;//列的值、直方图跨度、直方图数量

        StandardMapObjectInspector result;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            //2. 初始化变量
            assert (parameters.length == 3);
            super.init(m, parameters);

            // 指定各个阶段输出数据格式都为Map<Int,Int>类型
            ObjectInspector returnKey = PrimitiveObjectInspectorFactory
                    .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
            ObjectInspector returnValue = PrimitiveObjectInspectorFactory
                    .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);

            // 指定各个阶段输出数据格式都为Map<Int,Int>类型  注意不同阶段的输入值不同
            /**
             * 这里是整个UDAF最精华的部分
             * 从博客的图片我们可以看到在PARTIAL1阶段，调用init主要用来初始化iterate方法，也就是我们的计算过程
             * 在PARTIAL2和FINAL阶段，分别是中途合并mapper和结束mapper过程，都是调用merge函数，对中间的输出结果进行合并
             * 在COMPLETE阶段是最终输出结果
             * 因此，各个阶段要初始化的变量不同，尤其是parameters里面放的变量完全不同，PARTIAL1阶段，parameters就是输入值
             * PARTIAL2和FINAL阶段，parameters是输出值
             */
            //
            if (m == Mode.PARTIAL1) {
                colValueOI = (PrimitiveObjectInspector) parameters[0];
                stepSizeOI = (PrimitiveObjectInspector) parameters[1];
                histNumOI = (PrimitiveObjectInspector) parameters[2];
                return ObjectInspectorFactory.getStandardMapObjectInspector(returnKey, returnValue);
            } else if (m == Mode.PARTIAL2) {
                result = (StandardMapObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardMapObjectInspector(returnKey, returnValue);
            } else if (m == Mode.FINAL) {
                result = (StandardMapObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardMapObjectInspector(returnKey, returnValue);
            } else if (m == Mode.COMPLETE) {
                return ObjectInspectorFactory.getStandardMapObjectInspector(returnKey, returnValue);
            } else {
                throw new RuntimeException("no such mode Exception");
            }
        }

        /**
         * 存储当前中间变量的类
         */
        static class HistGramAgg implements AggregationBuffer {
            //3. 指明mapper计算的中间缓存值
            //此UDAF的中间缓存值就是记录频数的countMap
            Map<Integer, Integer> countMap = new HashMap<Integer, Integer>();

            //两个map相加
            void add(Map<?, ?> otherMap) {
                if (otherMap == null || otherMap.isEmpty()) return;

                //循环累加值
                for (Map.Entry<?, ?> entry : otherMap.entrySet()) {
                    Object key = (Object) entry.getKey();
                    Object value = (Object) entry.getValue();
                    int index = key instanceof Integer ? (Integer) key : ((IntWritable) key).get();
                    Integer otherCount = value instanceof Integer ? (Integer) value : ((IntWritable) value).get();
                    Integer beforeValue = countMap.get(index);//这个区间之前的数量
                    if (beforeValue == null) {
                        countMap.put(index, otherCount);
                    } else {
                        countMap.put(index, beforeValue + otherCount);
                    }
                }
            }
        }

        //创建新的聚合计算的需要的内存，用来存储mapper,combiner,reducer运算过程中的中间变量聚合值
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            //4. 新建mapper时，初始化中间缓存变量
            //本UDAF中，就是初始化countMap
            HistGramAgg myagg = new HistGramAgg();
            reset(myagg);
            return myagg;
        }

        //mapreduce支持mapper和reducer的重用，所以为了兼容，也需要做内存的重用。
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            //5. 重置中间缓存变量
            //本UDAF中，就是重置countMap
            HistGramAgg myagg = (HistGramAgg) agg;
            myagg.countMap = new HashMap<Integer, Integer>();
        }

        private boolean warned = false;

        //map阶段调用，实现计算逻辑
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            //6. 主体计算过程
            //在我看来，这反而是最简单的部分
            assert (parameters.length == 3);
            if (parameters[0] != null) {
                HistGramAgg myagg = (HistGramAgg) agg;
                //获取相应输入变量
                double colValue = PrimitiveObjectInspectorUtils.getDouble(parameters[0], colValueOI);
                double stepSize = PrimitiveObjectInspectorUtils.getDouble(parameters[1], stepSizeOI);
                int histNum = PrimitiveObjectInspectorUtils.getInt(parameters[2], histNumOI);

                Map<Integer, Integer> theCountMap = new HashMap<Integer, Integer>();
                //如果countMap为空先进行初始化
                if (myagg.countMap.isEmpty()) {
                    for (int i = 0; i < histNum; i++) {
                        theCountMap.put(i, 0);
                    }
                }
                //在对应的区间累加
                int index = (int) (colValue / stepSize);
                if (index >= histNum) index = histNum - 1;
                theCountMap.put(index, 1);
                myagg.add(theCountMap);
            }
        }

        //mapper结束要返回的结果，还有combiner结束返回的结果
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            //7. 合并mapper
            //调用add方法，目的在于合并两个mapper的中间缓存变量，此UDAF指的是countMap
            if (partial != null) {
                HistGramAgg myagg = (HistGramAgg) agg;
                Map<?, ?> countMap = result.getMap(partial);
                myagg.add(countMap);
            }
        }

        //reducer返回结果，或者是只有mapper，没有reducer时，在mapper端返回结果。
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            //8. 输出结果
            //返回最终的结果 countMap
            HistGramAgg myagg = (HistGramAgg) agg;
            return myagg.countMap;
        }
    }
}
