package boluo.work;

/* SimpleApp.java */
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchOperation;
import com.github.fge.jsonpatch.diff.JsonDiff;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class boluoTest01 {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        //String deviceInfo = "D:\\data\\dbv2\\y_device_info"; // Should be some file on your system
        String orderInfo = "D:\\data\\dbv2\\y_order_info";
        String orderParam = "D:\\data\\dbv2\\y_order_info_param";

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Simple Application")
                .getOrCreate();

        Dataset<Row> df1 = spark.read().load(orderInfo);
        //df1.printSchema();
        df1.show();

        Dataset<Row> df2 = spark.read().load(orderParam);
        //df2.printSchema();
        df2.show();

        df1.registerTempTable("b");
        df2.registerTempTable("a");

        /*Dataset ds = spark.sql("select aa.diiId as diiId, aa.diiNumber as diiNumber, aa.diiType as diiType, b.oi_date as oiDate\n" +
                "from b \n" +
                "right join \n" +
                "(\n" +
                "\tselect a.oip_oi_id, \n" +
                "\t\t   max(if(a.oip_key = 'dii_id', oip_value, null)) as diiId,\n" +
                "\t\t   max(if(a.oip_key = 'dii_number', oip_value, null)) as diiNumber,\n" +
                "\t\t   max(if(a.oip_key = 'dii_type', oip_value, null)) as diiType\n" +
                "\tfrom a \n" +
                "\twhere a.oip_key = 'dii_id' or a.oip_key = 'dii_number' or a.oip_key = 'dii_type'\n" +
                "\tgroup by a.oip_oi_id\n" +
                ") aa\n" +
                "on aa.oip_oi_id = b.oi_id");*/

        spark.udf().register("handleOrder", new UDF4<String, String, String, Boolean, String>() {

            @Override
            public String call(String diiId, String diiNumber, String diiType, Boolean active) throws Exception {
                List<String> list = new LinkedList<>();
                ObjectNode diiIdNode = mapper.createArrayNode()
                        .addObject()
                        .put("op", "replace")
                        .put("path", "/id")
                        .put("value", diiId);

                ObjectNode diiNumberNode = mapper.createArrayNode()
                        .addObject()
                        .put("op", "replace")
                        .put("path", "/code")
                        .put("value", diiNumber);

                ObjectNode diiTypeNode = mapper.createArrayNode()
                        .addObject()
                        .put("op", "replace")
                        .put("path", "/type")
                        .put("value", diiType == "1" ? "洗衣" : "烘干");

                ObjectNode activeNode = mapper.createArrayNode()
                        .addObject()
                        .put("op", "replace")
                        .put("path", "/active")
                        .put("value", active);

                list.add(diiIdNode.toString());
                list.add(diiNumberNode.toString());
                list.add(diiTypeNode.toString());
                list.add(activeNode.toString());

                return list.toString();
            }
        }, DataTypes.StringType);


        spark.udf().register("handleOrder2", new UDF4<String, String, String, Boolean, String>() {

            @Override
            public String call(String diiId, String diiNumber, String diiType, Boolean active) throws Exception {
                List<String> list = new LinkedList<>();
                ObjectNode diiIdNode = mapper.createArrayNode()
                        .addObject()
                        .put("op", "remove")
                        .put("path", "/id")
                        .put("value", diiId);

                ObjectNode diiNumberNode = mapper.createArrayNode()
                        .addObject()
                        .put("op", "replace")
                        .put("path", "/code")
                        .put("value", diiNumber);

                ObjectNode diiTypeNode = mapper.createArrayNode()
                        .addObject()
                        .put("op", "replace")
                        .put("path", "/type")
                        .put("value", diiType == "1" ? "洗衣" : "烘干");

                ObjectNode activeNode = mapper.createArrayNode()
                        .addObject()
                        .put("op", "replace")
                        .put("path", "/active")
                        .put("value", active);

                list.add(diiIdNode.toString());
                list.add(diiNumberNode.toString());
                list.add(diiTypeNode.toString());
                list.add(activeNode.toString());

                return list.toString();
            }
        }, DataTypes.StringType);

        /*spark.sql("select handleOrder(aa.diiId, aa.diiNumber, aa.diiType, true), b.oi_date\n" +
                "from b \n" +
                "right join \n" +
                "(\n" +
                "\tselect a.oip_oi_id, \n" +
                "\t\t   max(if(a.oip_key = 'dii_id', oip_value, null)) as diiId,\n" +
                "\t\t   max(if(a.oip_key = 'dii_number', oip_value, null)) as diiNumber,\n" +
                "\t\t   max(if(a.oip_key = 'dii_type', oip_value, null)) as diiType\n" +
                "\tfrom a \n" +
                "\twhere a.oip_key = 'dii_id' or a.oip_key = 'dii_number' or a.oip_key = 'dii_type'\n" +
                "\tgroup by a.oip_oi_id\n" +
                ") aa\n" +
                "on aa.oip_oi_id = b.oi_id")
                .write().mode(SaveMode.Overwrite)
                .csv("D:\\data\\demo\\boluo4");*/

        spark.sql("select handleOrder(aa.diiId, aa.diiNumber, aa.diiType, true), b.oi_date\n" +
                "from b \n" +
                "right join \n" +
                "(\n" +
                "\tselect a.oip_oi_id, \n" +
                "\t\t   max(if(a.oip_key = 'dii_id', oip_value, null)) as diiId,\n" +
                "\t\t   max(if(a.oip_key = 'dii_number', oip_value, null)) as diiNumber,\n" +
                "\t\t   max(if(a.oip_key = 'dii_type', oip_value, null)) as diiType\n" +
                "\tfrom a \n" +
                "\twhere a.oip_key = 'dii_id' or a.oip_key = 'dii_number' or a.oip_key = 'dii_type'\n" +
                "\tgroup by a.oip_oi_id\n" +
                ") aa\n" +
                "on aa.oip_oi_id = b.oi_id\n" +
                "\n" +
                "union \n" +
                "\n" +
                "select handleOrder2(aa.diiId, aa.diiNumber, aa.diiType, false), '2019-09-30'\n" +
                "from b \n" +
                "right join \n" +
                "(\n" +
                "\tselect a.oip_oi_id, \n" +
                "\t\t   max(if(a.oip_key = 'dii_id', oip_value, null)) as diiId,\n" +
                "\t\t   max(if(a.oip_key = 'dii_number', oip_value, null)) as diiNumber,\n" +
                "\t\t   max(if(a.oip_key = 'dii_type', oip_value, null)) as diiType\n" +
                "\tfrom a \n" +
                "\twhere a.oip_key = 'dii_id' or a.oip_key = 'dii_number' or a.oip_key = 'dii_type'\n" +
                "\tgroup by a.oip_oi_id\n" +
                ") aa\n" +
                "on aa.oip_oi_id = b.oi_id")
                .write().mode(SaveMode.Overwrite)
                .csv("D:\\data\\demo\\boluo4");

        //ds.write().mode(SaveMode.Overwrite).csv("D:\\data\\demo\\boluo3");
        spark.stop();
    }


}