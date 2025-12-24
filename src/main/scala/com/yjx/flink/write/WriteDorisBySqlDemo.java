package com.yjx.flink.write;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhang
 * @time 9/12/2025 下午5:39
 * @description
 */
public class WriteDorisBySqlDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello World!");
        System.out.println("hotfix - bug already hotfix!");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        String createTableSql = "CREATE TABLE flink_doris_source(\n" +
                "EMPNO int,\n" +
                "ENAME VARCHAR(255),\n" +
                "JOB VARCHAR(255),\n" +
                "MGR int,\n" +
                "HIREDATE date,\n" +
                "SAL decimal(10,0),\n" +
                "COMM decimal(10,0),\n" +
                "DEPTNO int\n" +
                ")\n" +
                "WITH(\n" +
                "'connector'='doris',\n" +
                "'fenodes'='node01:8030',\n" +
                "'table.identifier'='scott.emp',\n" +
                "'username'='root',\n" +
                "'password'='123456'\n" +
                ");";
        tableEnvironment.executeSql(createTableSql);
        tableEnvironment.executeSql("INSERT INTO flink_doris_source (EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, DEPTNO) VALUES(\n" +
                "8000,'zhangSan','Clear',7839,DATE '2000-01-01',2000,20" +
                ")");
    }
}
