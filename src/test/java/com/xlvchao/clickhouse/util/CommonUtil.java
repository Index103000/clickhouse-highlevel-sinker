package com.xlvchao.clickhouse.util;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by lvchao on 2022/7/4 16:06
 */
public class CommonUtil {

    /**
     * 生成随机AppId
     */

    private static Random random = new Random();

    private static List<String> products = new ArrayList<>();
    private static List<String> services = new ArrayList<>();
    private static List<String> itfs = new ArrayList<>();
    static {
        products.add("product1");
        products.add("product2");
        products.add("product3");
        products.add("product4");
        products.add("product5");

        services.add("service1");
        services.add("service2");
        services.add("service3");
        services.add("service4");
        services.add("service5");
        services.add("service6");
        services.add("service7");
        services.add("service8");
        services.add("service9");
        services.add("service10");

        itfs.add("itf1");
        itfs.add("itf2");
        itfs.add("itf3");
        itfs.add("itf4");
        itfs.add("itf5");
        itfs.add("itf6");
        itfs.add("itf7");
        itfs.add("itf8");
        itfs.add("itf9");
        itfs.add("itf10");
        itfs.add("itf11");
        itfs.add("itf12");
        itfs.add("itf13");
        itfs.add("itf14");
        itfs.add("itf15");
        itfs.add("itf16");
        itfs.add("itf17");
        itfs.add("itf18");
        itfs.add("itf19");
        itfs.add("itf20");
        itfs.add("itf21");
        itfs.add("itf22");
        itfs.add("itf23");
        itfs.add("itf24");
        itfs.add("itf25");
        itfs.add("itf26");
        itfs.add("itf27");
        itfs.add("itf28");
        itfs.add("itf29");
        itfs.add("itf30");
    }

    public static Map<String, String> genLogInfo() {
        Map<String, String> map = new HashMap<>();

        int idx1 = random.nextInt(4);
        int idx2 = random.nextInt(9);
        int idx3 = random.nextInt(29);

        map.put("product", products.get(idx1));
        map.put("service", services.get(idx2));
        map.put("itf", itfs.get(idx3));

        return map;
    }

    /**
     * 生成随机时间
     */
    public static Date randomDate(String beginDate, String endDate) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date start = format.parse(beginDate);// 构造开始日期
            Date end = format.parse(endDate);// 构造结束日期
            // getTime()表示返回自 1970 年 1 月 1 日 00:00:00 GMT 以来此 Date 对象表示的毫秒数。
            if (start.getTime() >= end.getTime()) {
                return null;
            }
            long date = random(start.getTime(), end.getTime());
            return new Date(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static long random(long begin, long end) {
        long rtn = begin + (long) (Math.random() * (end - begin + 1));
        // 如果返回的是开始时间和结束时间，则递归调用本函数查找随机值
        /*if (rtn == begin || rtn == end) {
            return random(begin, end);
        }*/
        return rtn;
    }


    public static void main(String[] args) {

        System.out.println(genLogInfo());
    }

}
