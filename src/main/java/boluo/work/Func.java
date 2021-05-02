package boluo.work;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class Func {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static Column patchFilter(String patch, String... retain) {
        return udf((UDF1<?, ?>) (String p) -> {

            ArrayNode a = mapper.createArrayNode();
            String pathValueStr = "";

            JsonNode jsonNode = mapper.readTree(p);

            for (JsonNode node : jsonNode) {
                switch (node.path("op").textValue()) {
                    case "replace":

                        String pathStr = node.findValue("path").asText();

                        if(!node.at("/value").isObject()){
                            boolean b = Arrays.stream(retain).anyMatch(i -> {
                                return (pathStr+"/").startsWith(i+"/");
                            });
                            if(b){
                                a.add(node);
                            }
                        } else {
                            // value是一个对象

                            List<String> list = new LinkedList<>();
                            for (String tempStr : retain) {
                                //boolean br = Arrays.stream(retain).anyMatch(i -> {
                                //    return (i+"/").startsWith(pathStr+"/");
                                //});

                                boolean br = (tempStr+"/").startsWith(pathStr+"/");

                                if (br) {
                                    list.add(tempStr.substring(pathStr.length()));
                                }
                            }
                            String[] strArr = list.toArray(new String[0]);

                            ObjectNode o = (ObjectNode) node.at("/value");

                            recursionOp(o, strArr);
                            if(o.size() != 0){
                                a.addObject()
                                        .put("op", node.findValue("op").asText())
                                        .put("path", node.path("path").asText())
                                        .set("value", o);
                            }
                        }
                        break;

                    default:
                        throw new UnsupportedOperationException("非add, remove, replace操作");

                }

            }

            return a.toString();
        }, StringType$.MODULE$).apply(expr(patch));
    }

    private static void recursionOp(ObjectNode obj, String... retainStrArr) {

        Map<String, String[]> p = new HashMap<>();
        String[] strArr;

        for (String retainStr : retainStrArr) {
            if (retainStr.length() == 0) {
                return;
            }
            int indexChar = retainStr.indexOf("/", 1);
            if(retainStr.charAt(0) != '/'){
                return ;
            }
            if (indexChar == -1) {
                p.put(retainStr, null);
            } else {
                List<String> listStr = new ArrayList<>();
                String preStr = retainStr.substring(0, indexChar);
                String aftStr = retainStr.substring(indexChar);
                listStr.add(aftStr);
                strArr = listStr.toArray(new String[0]);
                p.put(preStr, strArr);
            }
        }

        p.forEach((k, v) -> {
            if (!obj.at(k).isObject() || v == null) {
                return;
            }
            recursionOp((ObjectNode) obj.at(k), v);
        });

        List<String> tempList = p.keySet().stream().map(i -> {
            return i.substring(1);
        }).collect(Collectors.toList());

        obj.retain(tempList);
    }

    public void test(){

        String path = "/child";

        String s1 = path + "/foo/aaa/ccc";
        String s2 = path + "/foo/bbb";



    }

}

