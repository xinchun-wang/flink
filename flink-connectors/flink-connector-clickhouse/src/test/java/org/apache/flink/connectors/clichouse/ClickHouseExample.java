package org.apache.flink.connectors.clichouse;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ClickHouseExample {
	public static void main(String[] args) throws Exception {

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

		String TABLE_SOURCE_SQL = "CREATE TABLE mysql_machine_meta (" +
			" machine_id varchar, " +
			" time_stamp int, " +
			" failure_domain_1 int, " +
			" failure_domain_2 int, " +
			" cpu_num int," +
			" mem_size int," +
			" status varchar" +
			") with (" +
			" 'connector.type' = 'jdbc', " +
			" 'connector.url' = 'jdbc:mysql://10.189.108.110:3306/tyloo', " +
			" 'connector.table' = 'mysql_clusterdata_machine_meta', " +
			" 'connector.username' = 'root', " +
			" 'connector.username' = 'vip@mysql', " +
			" 'connector.driver' = 'com.mysql.jdbc.Driver' " +
			")";

		tEnv.sqlUpdate(TABLE_SOURCE_SQL);

		Table result = tEnv.sqlQuery("select * from mysql_machine_meta");

		DataStream<Row> resultSet = tEnv.toAppendStream(result, Row.class);
		resultSet.print();
		env.execute();


	}
}
