package cn.edu.tsinghua.iot.benchmark.cnosdb;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;

import java.util.List;

public class SqlBuilder {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final long TO_NANOSECOND =
      TimeUtils.getTimestampConst(config.getTIMESTAMP_PRECISION());

  private final StringBuilder builder;
  private int builderOriginalLength = 0;

  public SqlBuilder() {
    builder = new StringBuilder("SELECT");
    builderOriginalLength = builder.length();
  }

  public String build() {
    return builder.toString();
  }

  public void clear() {
    builder.setLength(builderOriginalLength);
  }

  public void newSql(List<DeviceSchema> deviceSchemas) {
    clear();
    builder.append(" time,");
    if (config.isALIGN_BY_DEVICE()) {
      builder.append(" device,");
    }
    if (!deviceSchemas.isEmpty()) {
      List<Sensor> sensors = deviceSchemas.get(0).getSensors();
      if (!sensors.isEmpty()) {
        for (Sensor sensor : sensors) {
          builder.append(" ").append(sensor.getName()).append(",");
        }
        builder.setLength(builder.length() - 1);
      }

      appendConstrain(deviceSchemas);
    }
  }

  public void newAggSql(List<DeviceSchema> deviceSchemas, String method) {
    clear();
    if (config.isALIGN_BY_DEVICE()) {
      builder.append(" device,");
    }
    if (!deviceSchemas.isEmpty()) {
      List<Sensor> sensors = deviceSchemas.get(0).getSensors();
      if (!sensors.isEmpty()) {
        for (Sensor sensor : sensors) {
          builder.append(" ").append(method).append("(").append(sensor.getName()).append("),");
        }
        builder.setLength(builder.length() - 1);
      }

      appendConstrain(deviceSchemas);
    }
  }

  public void newAggTimeRangeSql(
      List<DeviceSchema> deviceSchemas, String method, long granularity) {
    clear();
    builder
        .append(" DATE_BIN(INTERVAL '")
        .append(granularity)
        .append(config.getTIMESTAMP_PRECISION())
        .append("', time, TIMESTAMP '1970-01-01T00:00:00Z') AS time,");
    if (config.isALIGN_BY_DEVICE()) {
      builder.append(" device,");
    }
    if (!deviceSchemas.isEmpty()) {
      List<Sensor> sensors = deviceSchemas.get(0).getSensors();
      if (!sensors.isEmpty()) {
        for (Sensor sensor : sensors) {
          builder.append(" ").append(method).append("(").append(sensor.getName()).append("),");
        }
        builder.setLength(builder.length() - 1);
      }

      appendConstrain(deviceSchemas);
    }
  }

  public void newCountSql(DeviceSchema deviceSchema) {
    clear();
    builder.append(" COUNT(*) as row_count, CAST(MIN(time) AS BIGINT) as min_time, CAST(MAX(time) AS BIGINT) as max_time");
    appendConstrain(deviceSchema);
  }

  public void addWherePreciseTime(long timestamp) {
    builder.append(" AND time = ").append(timestamp * TO_NANOSECOND);
  }

  public void addWhereTimeRange(long startTimestamp, long endTimestamp) {
    builder
        .append(" AND time >= ")
        .append(startTimestamp * TO_NANOSECOND)
        .append(" AND time <=")
        .append(endTimestamp * TO_NANOSECOND);
  }

  public void addWhereValueRange(List<Sensor> sensors, double valueThreshold) {
    if (!sensors.isEmpty()) {
      for (Sensor sensor : sensors) {
        builder.append(" AND ").append(sensor.getName()).append(" > ").append(valueThreshold);
      }
    }
  }

  public void addGroupByTimeRange(long granularity) {
    builder
        .append(" GROUP BY DATE_BIN(INTERVAL '")
        .append(granularity)
        .append(config.getTIMESTAMP_PRECISION())
        .append("', time, TIMESTAMP '1970-01-01T00:00:00Z')");
  }

  public void addOrderByDesc() {
    builder.append(" ORDER BY time DESC");
  }

  private void appendConstrain(DeviceSchema deviceSchema) {
    builder.append(" FROM ").append(deviceSchema.getGroup())
        .append(" WHERE device = '")
        .append(deviceSchema.getDevice()).append("'");
  }

  private void appendConstrain(List<DeviceSchema> deviceSchemas) {
    builder.append(" FROM");
    for (DeviceSchema deviceSchema : deviceSchemas) {
      builder.append(" ").append(deviceSchema.getGroup()).append(",");
    }
    builder.setLength(builder.length() - 1);

    builder.append(" WHERE (");
    for (DeviceSchema deviceSchema : deviceSchemas) {
      builder.append("device = '").append(deviceSchema.getDevice()).append("' OR");
    }
    builder.setLength(builder.length() - 3);
    builder.append(")");
  }

  private void appendTail() {
    if (config.getRESULT_ROW_LIMIT() >= 0) {
      builder.append(" LIMIT ").append(config.getRESULT_ROW_LIMIT());
    }
  }
}
