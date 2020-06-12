package org.apache.flink.connectors.clickhouse;

import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ClickHouseJDBCValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickhouseJdbcUrlParser;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ClickHouseJDBCValidator.CONNECTOR_TABLE;
import static org.apache.flink.table.descriptors.ClickHouseJDBCValidator.CONNECTOR_TYPE_VALUE_CLICKHOUSE;
import static org.apache.flink.table.descriptors.ClickHouseJDBCValidator.CONNECTOR_URL;
import static org.apache.flink.table.descriptors.ClickHouseJDBCValidator.CONNECTOR_CLUSTER;
import static org.apache.flink.table.descriptors.ClickHouseJDBCValidator.CONNECTOR_USERNAME;
import static org.apache.flink.table.descriptors.ClickHouseJDBCValidator.CONNECTOR_PASSWORD;
import static org.apache.flink.table.descriptors.ClickHouseJDBCValidator.CONNECTOR_WRITE_FLUSH_MAX_ROWS;
import static org.apache.flink.table.descriptors.ClickHouseJDBCValidator.CONNECTOR_WRITE_FLUSH_INTERVAL;
import static org.apache.flink.table.descriptors.ClickHouseJDBCValidator.CONNECTOR_WRITE_MAX_RETRIES;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_EXPR;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating configured instances of {@link JDBCUpsertTableSink}.
 */
public class ClickHouseTableSinkFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {
	private static final Logger LOG = LoggerFactory.getLogger(ClickHouseTableSinkFactory.class);

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_CLICKHOUSE); // clickhouse
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// common options
		properties.add(CONNECTOR_URL);
		properties.add(CONNECTOR_CLUSTER);
		properties.add(CONNECTOR_TABLE);
		properties.add(CONNECTOR_USERNAME);
		properties.add(CONNECTOR_PASSWORD);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);
		// computed column
		properties.add(SCHEMA + ".#." + TABLE_SCHEMA_EXPR);

		return properties;
	}


	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema schema = TableSchemaUtils.getPhysicalSchema(
			descriptorProperties.getTableSchema(SCHEMA));

		final JDBCUpsertTableSink.Builder builder = JDBCUpsertTableSink.builder()
			.setOptions(getJDBCOptions(descriptorProperties))
			.setTableSchema(schema);

		descriptorProperties.getOptionalInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS).ifPresent(builder::setFlushMaxSize);
		descriptorProperties.getOptionalDuration(CONNECTOR_WRITE_FLUSH_INTERVAL).ifPresent(
			s -> builder.setFlushIntervalMills(s.toMillis()));
		descriptorProperties.getOptionalInt(CONNECTOR_WRITE_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);

		return builder.build();
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new SchemaValidator(true, false, false).validate(descriptorProperties);
		new ClickHouseJDBCValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	private JDBCOptions getJDBCOptions(DescriptorProperties descriptorProperties) {
		final String url = descriptorProperties.getString(CONNECTOR_URL);
		final JDBCOptions.Builder builder = JDBCOptions.builder()
			.setDBUrl(url)
			.setTableName(descriptorProperties.getString(CONNECTOR_TABLE))
			.setDialect(new ClickHouseDialect());

		descriptorProperties.getOptionalString(CONNECTOR_USERNAME).ifPresent(builder::setUsername);
		descriptorProperties.getOptionalString(CONNECTOR_PASSWORD).ifPresent(builder::setPassword);

		if(descriptorProperties.getOptionalString(CONNECTOR_CLUSTER).isPresent()){
			builder.setSharding(true);
			String dbURL = getClusterConnUrl(descriptorProperties, descriptorProperties.getString(CONNECTOR_CLUSTER));
			builder.setDBUrl(dbURL);
		}

		return builder.build();
	}

	protected String getClusterConnUrl(DescriptorProperties descriptorProperties, String cluster){
		BalancedClickhouseDataSource dataSource;

		if(descriptorProperties.getOptionalString(CONNECTOR_USERNAME).isPresent()) {
			Properties properties = new Properties();
			properties.setProperty("user", descriptorProperties.getString(CONNECTOR_USERNAME));
			properties.setProperty("password", descriptorProperties.getString(CONNECTOR_PASSWORD));
			dataSource = new BalancedClickhouseDataSource(descriptorProperties.getString(CONNECTOR_URL), properties);
		}
		else{
			dataSource = new BalancedClickhouseDataSource(descriptorProperties.getString(CONNECTOR_URL));
		}

		List<String> urls = new ArrayList<>();
		try {
			ClickHouseConnection clickHouseConnection = dataSource.getConnection();

			String sql = String.format(
				"SELECT cluster, shard_num, shard_weight, host_address FROM system.clusters WHERE cluster = '%s' AND replica_num = 1",
				cluster);
			ResultSet rs = clickHouseConnection.createStatement().executeQuery(sql);

			while (rs.next()) {
				int shardWeight = rs.getInt("shard_weight");
				for(int i = 0; i < shardWeight; i++){
					String hostAddress = rs.getString("host_address");
					String url = getConnUrl(descriptorProperties.getString(CONNECTOR_URL), hostAddress);
					urls.add(url);

					LOG.info("prepare cluster's jdbc connection url, the real url is [{}]", url);
				}
			}
		}
		catch (Exception e){
			throw new RuntimeException("query shard from clickhouse cluster table error.", e);
		}
		String formattedUrls = String.join(";", urls);
		return formattedUrls;
	}

	private String getConnUrl(String connectorURL, String host) throws URISyntaxException{
		ClickHouseProperties clickHouseProperties = ClickhouseJdbcUrlParser.parse(connectorURL, new Properties());
		int port = clickHouseProperties.getPort();
		String database = clickHouseProperties.getDatabase();
		String connUrl = String.format("jdbc:clickhouse://%s:%s/%s", host, port, database);
		return connUrl;
	}

}
