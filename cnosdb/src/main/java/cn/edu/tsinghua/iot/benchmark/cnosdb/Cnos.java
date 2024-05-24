package cn.edu.tsinghua.iot.benchmark.cnosdb;

import cn.edu.tsinghua.iot.benchmark.cnosdb.api.CnosdbConnection;
import cn.edu.tsinghua.iot.benchmark.cnosdb.api.CnosdbResponse;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.DeviceSummary;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Cnos implements IDatabase {
  private static final Logger logger = LoggerFactory.getLogger(Cnos.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private CnosdbConnection connection;

  @Override
  public void init() throws TsdbException {}

  @Override
  public void cleanup() throws TsdbException {}

  @Override
  public void close() throws TsdbException {}

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    return 0.0;
  }

  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    return null;
  }

  private Status executeQuery(String sql) {
    CnosdbResponse response = connection.executeQuery(sql);
    if (!response.isOk()) {
      closeResponse(response);
      return new Status(false, response.getFailException(), response.getErrorMessage());
    }
    long cnt = 0;
    int columns = response.getResponseTitles().length - 2;
    while (response.hasNext()) {
      response.next();
      cnt += columns;
    }
    if (!config.isIS_QUIET_MODE()) {
      logger.debug("{} 查到数据点数: {}", Thread.currentThread().getName(), cnt);
    }
    closeResponse(response);
    return new Status(true, cnt);
  }

  private void closeResponse(CnosdbResponse response) {
    try {
      response.close();
    } catch (IOException e) {
      logger.error("Failed close cnosdb response", e);
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    SqlBuilder sqlBuilder = new SqlBuilder();
    sqlBuilder.newSql(preciseQuery.getDeviceSchema());
    sqlBuilder.addWherePreciseTime(preciseQuery.getTimestamp());
    String sql = sqlBuilder.build();
    if (!config.isIS_QUIET_MODE()) {
      logger.info("{} precise_query: {}", Thread.currentThread().getName(), sql);
    }
    return executeQuery(sql);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    SqlBuilder sqlBuilder = new SqlBuilder();
    sqlBuilder.newSql(rangeQuery.getDeviceSchema());
    sqlBuilder.addWhereTimeRange(rangeQuery.getStartTimestamp(), rangeQuery.getEndTimestamp());
    String sql = sqlBuilder.build();
    if (!config.isIS_QUIET_MODE()) {
      logger.info("{} range_query: {}", Thread.currentThread().getName(), sql);
    }
    return executeQuery(sql);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    SqlBuilder sqlBuilder = new SqlBuilder();
    List<DeviceSchema> deviceSchemas = valueRangeQuery.getDeviceSchema();
    sqlBuilder.newSql(valueRangeQuery.getDeviceSchema());
    sqlBuilder.addWhereTimeRange(
        valueRangeQuery.getStartTimestamp(), valueRangeQuery.getEndTimestamp());
    sqlBuilder.addWhereValueRange(
        deviceSchemas.get(0).getSensors(), valueRangeQuery.getValueThreshold());
    String sql = sqlBuilder.build();
    if (!config.isIS_QUIET_MODE()) {
      logger.info("{} value_range_query: {}", Thread.currentThread().getName(), sql);
    }
    return executeQuery(sql);
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    SqlBuilder sqlBuilder = new SqlBuilder();
    sqlBuilder.newAggSql(aggRangeQuery.getDeviceSchema(), aggRangeQuery.getAggFun());
    sqlBuilder.addWhereTimeRange(
        aggRangeQuery.getStartTimestamp(), aggRangeQuery.getEndTimestamp());
    String sql = sqlBuilder.build();
    if (!config.isIS_QUIET_MODE()) {
      logger.info("{} agg_range_query: {}", Thread.currentThread().getName(), sql);
    }
    return executeQuery(sql);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    SqlBuilder sqlBuilder = new SqlBuilder();
    List<DeviceSchema> deviceSchemas = aggValueQuery.getDeviceSchema();
    sqlBuilder.newAggSql(deviceSchemas, aggValueQuery.getAggFun());
    sqlBuilder.addWhereValueRange(
        deviceSchemas.get(0).getSensors(), aggValueQuery.getValueThreshold());
    String sql = sqlBuilder.build();
    if (!config.isIS_QUIET_MODE()) {
      logger.info("{} agg_value_query: {}", Thread.currentThread().getName(), sql);
    }
    return executeQuery(sql);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    SqlBuilder sqlBuilder = new SqlBuilder();
    List<DeviceSchema> deviceSchemas = aggRangeValueQuery.getDeviceSchema();
    sqlBuilder.newAggSql(deviceSchemas, aggRangeValueQuery.getAggFun());
    sqlBuilder.addWhereTimeRange(
        aggRangeValueQuery.getStartTimestamp(), aggRangeValueQuery.getEndTimestamp());
    sqlBuilder.addWhereValueRange(
        deviceSchemas.get(0).getSensors(), aggRangeValueQuery.getEndTimestamp());
    String sql = sqlBuilder.build();
    if (!config.isIS_QUIET_MODE()) {
      logger.info("{} agg_range_value_query: {}", Thread.currentThread().getName(), sql);
    }
    return executeQuery(sql);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    SqlBuilder sqlBuilder = new SqlBuilder();
    sqlBuilder.newAggTimeRangeSql(
        groupByQuery.getDeviceSchema(), groupByQuery.getAggFun(), groupByQuery.getGranularity());
    sqlBuilder.addWhereTimeRange(groupByQuery.getStartTimestamp(), groupByQuery.getEndTimestamp());
    sqlBuilder.addGroupByTimeRange(groupByQuery.getGranularity());
    String sql = sqlBuilder.build();
    if (!config.isIS_QUIET_MODE()) {
      logger.info("{} group_by_query: {}", Thread.currentThread().getName(), sql);
    }
    return executeQuery(sql);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    SqlBuilder sqlBuilder = new SqlBuilder();
    sqlBuilder.newAggSql(latestPointQuery.getDeviceSchema(), "last");
    String sql = sqlBuilder.build();
    if (!config.isIS_QUIET_MODE()) {
      logger.info("{} latest_point_query: {}", Thread.currentThread().getName(), sql);
    }
    return executeQuery(sql);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    SqlBuilder sqlBuilder = new SqlBuilder();
    sqlBuilder.newSql(rangeQuery.getDeviceSchema());
    sqlBuilder.addWhereTimeRange(rangeQuery.getStartTimestamp(), rangeQuery.getEndTimestamp());
    sqlBuilder.addOrderByDesc();
    String sql = sqlBuilder.build();
    if (!config.isIS_QUIET_MODE()) {
      logger.info("{} range_query_order_by_desc: {}", Thread.currentThread().getName(), sql);
    }
    return executeQuery(sql);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    SqlBuilder sqlBuilder = new SqlBuilder();
    List<DeviceSchema> deviceSchemas = valueRangeQuery.getDeviceSchema();
    sqlBuilder.newSql(deviceSchemas);
    sqlBuilder.addWhereTimeRange(
        valueRangeQuery.getStartTimestamp(), valueRangeQuery.getEndTimestamp());
    sqlBuilder.addWhereValueRange(
        deviceSchemas.get(0).getSensors(), valueRangeQuery.getValueThreshold());
    sqlBuilder.addOrderByDesc();
    String sql = sqlBuilder.build();
    if (!config.isIS_QUIET_MODE()) {
      logger.info("{} value_range_query_order_by_desc: {}", Thread.currentThread().getName(), sql);
    }
    return executeQuery(sql);
  }

  @Override
  public Status groupByQueryOrderByDesc(GroupByQuery groupByQuery) {
    SqlBuilder sqlBuilder = new SqlBuilder();
    List<DeviceSchema> deviceSchemas = groupByQuery.getDeviceSchema();
    sqlBuilder.newAggSql(deviceSchemas, groupByQuery.getAggFun());
    sqlBuilder.addWhereTimeRange(
        groupByQuery.getStartTimestamp(), groupByQuery.getEndTimestamp());
    sqlBuilder.addGroupByTimeRange(groupByQuery.getGranularity());
    sqlBuilder.addOrderByDesc();
    String sql = sqlBuilder.build();
    if (!config.isIS_QUIET_MODE()) {
      logger.info("{} group_by_query_order_by_desc: {}", Thread.currentThread().getName(), sql);
    }
    return executeQuery(sql);
  }

  @Override
  public Status verificationQuery(VerificationQuery verificationQuery) {
    Instant startInstant = Instant.now();

    DeviceSchema deviceSchema = verificationQuery.getDeviceSchema();
    List<Sensor> sensors = deviceSchema.getSensors();
    if (sensors == null) {
      sensors = new ArrayList<>(0);
    }
    List<Record> records = verificationQuery.getRecords();
    if (records == null || records.isEmpty()) {
      return new Status(
          false,
          new TsdbException("There are no records in verificationQuery."),
          "There are no records in verificationQuery.");
    }
    long minTimestampExpected = Long.MAX_VALUE;
    long maxTimestampExpected = Long.MIN_VALUE;
    for (Record record : records) {
      minTimestampExpected = Math.min(record.getTimestamp(), minTimestampExpected);
      maxTimestampExpected = Math.max(record.getTimestamp(), maxTimestampExpected);
    }

    SqlBuilder sqlBuilder = new SqlBuilder();
    sqlBuilder.newCountSql(deviceSchema);
    sqlBuilder.addWhereTimeRange(minTimestampExpected, maxTimestampExpected);
    String countSql = sqlBuilder.build();
    CnosdbResponse response = connection.executeQuery(countSql);
    if (!response.isOk()) {
      closeResponse(response);
      return new Status(false, response.getFailException(), response.getErrorMessage());
    }
    long countFromQuery = 0L;
    long minTimestampFromQuery = Long.MAX_VALUE;
    long maxTimestampFromQuery = Long.MIN_VALUE;
    if (response.hasNext()) {
      String[] resultSet = response.next();
      countFromQuery = Long.parseLong(resultSet[0]);
      minTimestampFromQuery = Long.parseLong(resultSet[1]);
      maxTimestampFromQuery = Long.parseLong(resultSet[2]);
    }

    if (countFromQuery < minTimestampFromQuery) {
      
    }


  }

  @Override
  public Status deviceQuery(DeviceQuery deviceQuery) throws SQLException, TsdbException {

  }

  @Override
  public DeviceSummary deviceSummary(DeviceQuery deviceQuery) throws SQLException, TsdbException {

  }
}
