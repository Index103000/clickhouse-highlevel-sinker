package com.xlvchao.clickhouse.util;

import com.xlvchao.clickhouse.annotation.TableName;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

/**
 * 将对象(or对象集合)转成批量执行的sql脚本
 *
 * String s = TableUtil
 *          .create(log)
 *          .create(logs)
 *          .fieldsAll() //转换全部列
 *          .addIncludeFields("field1","filed2") //只包含某些列
 *          .addExcludeFields("field1","filed2") //排除某些列
 *          .addHead((data) -> "hello") //添加头部字段
 *          .addContent() //一个对象(一行)
 *          .addContents() //多个对象(多行)
 *          .addEnding((data) -> "world")
 *          .ToString(); //结果
 *
 * Created by lvchao on 2022/8/1 16:54
 */
public class TableUtil<T> {

    private static String separator = ",";  //默认分隔符
    private StringBuilder str = new StringBuilder();
    private T object;
    private List<T> objects;
    private List<String> excludeFields = new ArrayList<String>(100) {{
        add("serialVersionUID");
    }};
    private List<String> includeFields = new ArrayList<String>(100);
    private boolean fieldsAll = false;
    private  int lineNumber; //行数

    private TableUtil(T object, String separator) {
        this.object = object;
        this.separator = separator;
        //行数
        this.lineNumber =1;
    }
    private TableUtil(T object) {
        this.object = object;
        //行数
        this.lineNumber =1;

    }
    private TableUtil(List<T> objects) {
        if (objects.isEmpty()) {
            throw new NullPointerException("不能是空");
        }
        this.object = objects.get(0);
        this.objects = objects;
        //行数
        this.lineNumber =objects.size();
    }
    private TableUtil(List<T> objects, String separator) {
        if (objects.isEmpty()) {
            throw new NullPointerException("不能是空");
        }
        this.object = objects.get(0);
        this.objects = objects;
        this.separator = separator;
        //行数
        this.lineNumber =objects.size();

    }

    //排除和包含都有,那么以包含的为主
    private boolean decideFields(String fieldName) {
        //包含
        if (includeFields.contains(fieldName)) {
            return false;
        }
        //排除
        if (excludeFields.contains(fieldName)) {
            return true;
        }
        // 开启全部放行
        if (fieldsAll) {
            return false;
        }
        //默认拦截全部
        return true;
    }

    public static <T> TableUtil<T> create(T object, String separator) {
        return new TableUtil<T>(object, separator);
    }
    public static <T> TableUtil<T> create(T object) {
        return new TableUtil<T>(object);
    }
    public static <T> TableUtil<T> create(List<T> object, String separator) {
        return new TableUtil<T>(object, separator);
    }
    public static <T> TableUtil<T> create(List<T> object) {
        return new TableUtil<T>(object);
    }

    //全部放行
    public TableUtil<T> fieldsAll() {
        fieldsAll = true;
        return this;
    }

    //包含
    public TableUtil<T> addIncludeFields(String... fieldName) {
        includeFields.addAll(Arrays.asList(fieldName));
        return this;
    }

    //排除
    public TableUtil<T> addExcludeFields(String... fieldName) {
        excludeFields.addAll(Arrays.asList(fieldName));
        return this;
    }

    //添加头部
    public TableUtil<T> addHead(Function<TableUtil, String> functor) {
        String apply = functor.apply( this);
        this.str.append(apply);
        return this;
    }

    //天假尾部
    public TableUtil<T> addEnding(Function<TableUtil, String> functor){
        String apply = functor.apply( this);
        this.str.append(apply);
        return this;
    }

    public TableUtil<T> addContent() throws IllegalAccessException {
        StringBuilder str1 = new StringBuilder();
        Class<?> aClass = this.object.getClass();
        Field[] fields = aClass.getDeclaredFields();
        int length = fields.length;
        for (int i = 0; i < length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            if (decideFields(field.getName())) {
                continue;
            }

            Class type = field.getType();
            Object o = field.get(object);
            String value = "";
            if (o != null) {
                if (type == String.class){
                    value = "'" + String.valueOf(o) + "'";
                } else if(type == Date.class) {
                    value = "'" + DateTimeUtil.formatDate((Date) o) + "'";
                } else {
                    value = String.valueOf(o);
                }
            } else {
                value = null;
            }
            str1.append(value).append(separator);
        }
        int length1 = separator.length();
        String substring = str1.substring(0, str1.length() - length1);
        this.str.append("(").append(substring).append(")");

        return this;
    }

    public TableUtil<T> addContents() throws IllegalAccessException {
        for (T t : this.objects) {
            this.object=t;
            addContent();
            this.str.append(separator);
        }
        this.str.deleteCharAt(this.str.length()-1);
        return this;
    }

    public String ToString() {
        return str.toString();
    }

    public static String getTableColumns(Class clazz) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");

        StringBuilder sbTemp = new StringBuilder();
        Field[] fields = clazz.getDeclaredFields();
        int length = fields.length;
        for (int i = 0; i < length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            sbTemp.append(field.getName()).append(separator);
        }
        String substring = sbTemp.substring(0, sbTemp.length() - separator.length());

        sb.append(substring).append(")");
        return sb.toString();
    }

    public static String genSqlTemplate(Class clazz) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s %s VALUES ", getTableName(clazz), getTableColumns(clazz)));

        StringBuilder sbTemp = new StringBuilder();
        Field[] fields = clazz.getDeclaredFields();
        int length = fields.length;
        for (int i = 0; i < length; i++) {
            sbTemp.append("?").append(separator);
        }
        String substring = sbTemp.substring(0, sbTemp.length() - separator.length());

        sb.append("(").append(substring).append(")");
        return sb.toString();
    }

    public static String getTableName(Class clazz) {
        if (clazz.isAnnotationPresent(TableName.class)) {
            TableName declaredAnnotation = (TableName) clazz.getDeclaredAnnotation(TableName.class);
            return declaredAnnotation.value();
        }
        return null;
    }

    /*public static void main(String[] args) {
        System.out.println(genSqlTemplate(InterfaceLog.class));
    }*/
}
