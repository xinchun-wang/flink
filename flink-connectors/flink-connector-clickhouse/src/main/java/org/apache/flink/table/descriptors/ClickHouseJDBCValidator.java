package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

/**
 * The validator for ClickHouse.
 */
@Internal
public class ClickHouseJDBCValidator extends ConnectorDescriptorValidator{
	public static final String CONNECTOR_TYPE_VALUE_CLICKHOUSE = "clickhouse";

	public static final String CONNECTOR_URL = "connector.url";
	public static final String CONNECTOR_CLUSTER = "connector.cluster";
	public static final String CONNECTOR_TABLE = "connector.table";
	public static final String CONNECTOR_USERNAME = "connector.username";
	public static final String CONNECTOR_PASSWORD = "connector.password";

	public static final String CONNECTOR_WRITE_FLUSH_MAX_ROWS = "connector.write.flush.max-rows";
	public static final String CONNECTOR_WRITE_FLUSH_INTERVAL = "connector.write.flush.interval";
	public static final String CONNECTOR_WRITE_MAX_RETRIES = "connector.write.max-retries";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		validateCommonProperties(properties);
		validateSinkProperties(properties);
	}

	private void validateCommonProperties(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_URL, false, 1);
		properties.validateString(CONNECTOR_TABLE, false, 1);
		properties.validateString(CONNECTOR_CLUSTER, true);
		properties.validateString(CONNECTOR_USERNAME, true);
		properties.validateString(CONNECTOR_PASSWORD, true);

		Optional<String> password = properties.getOptionalString(CONNECTOR_PASSWORD);
		if (password.isPresent()) {
			Preconditions.checkArgument(
				properties.getOptionalString(CONNECTOR_USERNAME).isPresent(),
				"Database username must be provided when database password is provided");
		}
	}

	private void validateSinkProperties(DescriptorProperties properties) {
		properties.validateInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS, true);
		properties.validateDuration(CONNECTOR_WRITE_FLUSH_INTERVAL, true, 1);
		properties.validateInt(CONNECTOR_WRITE_MAX_RETRIES, true);
	}

}
