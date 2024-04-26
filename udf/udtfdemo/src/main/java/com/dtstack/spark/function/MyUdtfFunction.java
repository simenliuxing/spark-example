package com.dtstack.spark.function;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import java.util.ArrayList;
/**
 * @Author longxuan
 * @Create 2021/12/13 09:58
 * @Description
 */
public class MyUdtfFunction extends GenericUDTF {
    /**
     * @param args 输入要初始化的字符串
     * @return
     */

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException{
        if(args.length!=1){
            throw new UDFArgumentLengthException("ExplodeMap takes only one argument");

        }
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
        }
        ArrayList<String> fieldNames=new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs=new ArrayList<ObjectInspector>();
        fieldNames.add("col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    /**
     * @param args 输入要分解的字符串，取第一个是传入的字符串
     * @return
     */

    @Override
    public void process(Object[] args) throws HiveException {
        String input=args[0].toString();
        String[] test=input.split(";");
        for(int i=0;i<test.length;i++){
            try {
                String[] result = test[i].split(":");
                forward(result);
            }catch (Exception e){
                continue;
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
