package com.wmy.flink.warehourse.func;

import com.wmy.flink.warehourse.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * ClassName:KeyWordUDTF
 * Package:com.wmy.flink.warehourse.func
 *
 * @date:2021/7/25 10:23
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description:
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeyWordUDTF extends TableFunction<Row> {

    public void eval(String str) {

        //使用IK分词器对搜索的关键词进行分词处理
        List<String> list = KeyWordUtil.analyze(str);

        //遍历单词写出
        for (String word : list) {
            collect(Row.of(word));
        }
    }
}
