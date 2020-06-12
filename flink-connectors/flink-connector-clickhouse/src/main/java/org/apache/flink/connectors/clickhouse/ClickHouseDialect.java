package org.apache.flink.connectors.clickhouse;

import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;

import java.util.Optional;

public class ClickHouseDialect implements JDBCDialect {
	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:clickhouse:");
	}

	@Override
	public Optional<String> defaultDriverName() {
		return Optional.of("ru.yandex.clickhouse.ClickHouseDriver");
	}

}
