import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhang
 * @time 9/12/2025 下午7:12
 * @description
 */
public class FlinkDorisDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        String createTableSql = "CREATE TABLE flink_doris_source\n(" +
                "EMPNO int, \n" +
                "ENAME varchar(255),\n" +
                "JOB varchar(255),\n" +
                "MGR int ,\n" +
                "HIREDATE date,\n" +
                "SAL decimal(10,0),\n" +
                "COMM decimal(10,0),\n" +
                "DEPTNO int \n" +
                ")" +
                "WITH(\n" +
                "'connector'='doris'," +
                "'table.identifier'='scott.emp'," +
                "'fenodes'='node01:8030'," +
                "'username'='root'," +
                "'password'='123456'" +
                ")";

        tableEnvironment.executeSql(createTableSql);
        Table table = tableEnvironment.sqlQuery("select * from flink_doris_source");
        table.execute().print();
        environment.execute();
    }
}
