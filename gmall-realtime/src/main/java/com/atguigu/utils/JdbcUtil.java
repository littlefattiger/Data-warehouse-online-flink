package com.atguigu.utils;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel)
            throws Exception {
        ArrayList<T> resultList = new ArrayList<>();
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);


        ResultSet resultSet = preparedStatement.executeQuery();


        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()){
            T t = clz.newInstance();
            for (int i = 1; i < columnCount + 1; i++) {
                String columnName = metaData.getColumnName(i);

                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE
                            .to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                Object value = resultSet.getObject(i);

                BeanUtils.setProperty(t, columnName, value);

            }
            resultList.add(t);
        }

        preparedStatement.close();
        resultSet.close();
        return resultList;
    }
}
