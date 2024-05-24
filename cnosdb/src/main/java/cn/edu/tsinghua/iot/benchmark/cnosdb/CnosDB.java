/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.cnosdb;

import cn.edu.tsinghua.iot.benchmark.cnosdb.api.CnosdbConnection;
import cn.edu.tsinghua.iot.benchmark.cnosdb.api.CnosdbResponse;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.influxdb.InfluxDB;
import cn.edu.tsinghua.iot.benchmark.influxdb.InfluxDataModel;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.VerificationQuery;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.dto.BatchPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CnosDB extends InfluxDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(CnosDB.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private final String cnosdbUrl;
  private final String cnosdbUsername;
  private final String cnosdbPassword;
  private final String cnosdbDatabase;

  private org.influxdb.InfluxDB influxDbInstance;
  private CnosdbConnection connection;
  private static final long TIMESTAMP_TO_NANO =
      InfluxDB.getToNanoConst(config.getTIMESTAMP_PRECISION());

  private ThreadLocal<StringBuilder> buffer;

  /** constructor. */
  public CnosDB(DBConfig dbConfig) {
    super(dbConfig);
    this.cnosdbUrl = "http://" + dbConfig.getHOST().get(0) + ":" + dbConfig.getPORT().get(0);
    this.cnosdbUsername = dbConfig.getUSERNAME();
    this.cnosdbPassword = dbConfig.getPASSWORD();
    this.cnosdbDatabase = dbConfig.getDB_NAME();
    this.buffer = new ThreadLocal<>();
  }

  @Override
  public void init() throws TsdbException {
    try {
      Builder client =
          new Builder()
              .connectTimeout(5, TimeUnit.MINUTES)
              .readTimeout(5, TimeUnit.MINUTES)
              .writeTimeout(5, TimeUnit.MINUTES)
              .retryOnConnectionFailure(true);
      influxDbInstance = org.influxdb.InfluxDBFactory.connect(cnosdbUrl, client);
      connection = new CnosdbConnection(cnosdbUrl, cnosdbUsername, cnosdbPassword, cnosdbDatabase);
    } catch (Exception e) {
      LOGGER.error("Initialize CnosDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    try {
      Response response = connection.executeDdl("DROP DATABASE IF EXISTS \"" + database + "\"");
      response.close();
    } catch (Exception e) {
      LOGGER.error("Cleanup CnosDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void close() {
    influxDbInstance.close();
  }

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    String sql =
        String.format(
            "CREATE DATABASE IF NOT EXISTS \"%s\" WITH SHARD %d",
            database, config.getCNOSDB_SHARD_NUMBER());
    long start = System.nanoTime();
    int tryCount = 0;
    Exception lastException = null;
    while (tryCount < 3) {
      try (CnosdbResponse response = connection.executeDdl(sql)) {
        if (response.isOk()) {
          long end = System.nanoTime();
          return TimeUtils.convertToSeconds(end - start, "ns");
        }
        tryCount++;
        lastException = response.getFailException();
      } catch (Exception e) {
        LOGGER.error("Register schema for CnosDB failed: {}", e.getMessage());
        tryCount++;
        lastException = e;
      }
    }
    throw new TsdbException(lastException);
  }

  @Override
  public Status insertOneBatch(IBatch batch) {
    BatchPoints batchPoints = BatchPoints.database(cnosdbDatabase).build();
    try {
      InfluxDataModel model;
      for (Record record : batch.getRecords()) {
        model =
            super.createDataModel(
                batch.getDeviceSchema(), record.getTimestamp(), record.getRecordDataValue());
        batchPoints.point(model.toInfluxPoint());
      }

      influxDbInstance.write(batchPoints);

      return new Status(true);
    } catch (Exception e) {
      LOGGER.warn(e.getMessage());
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status groupByQueryOrderByDesc(GroupByQuery groupByQuery) {
    String sql = getAggQuerySqlHead(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
    sql = InfluxDB.addWhereTimeClause(sql, groupByQuery);
    sql = addGroupByClause(sql, groupByQuery.getGranularity());
    return addTailClausesAndExecuteQueryAndGetStatus(sql);
  }

  @Override
  public Status addTailClausesAndExecuteQueryAndGetStatus(String sql) {
    if (config.getRESULT_ROW_LIMIT() >= 0) {
      sql += " LIMIT " + config.getRESULT_ROW_LIMIT();
    }
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), sql);
    }

    try (CnosdbResponse response = connection.executeDdl(sql)) {
      long cnt = 0;
      BufferedReader bufferedReader =
          new BufferedReader(new InputStreamReader(response.body().byteStream()));
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<String, Object> hashMap =
            objectMapper.readValue(line, new TypeReference<HashMap<String, Object>>() {});
        cnt += hashMap.size();
      }
      response.close();
      if (!config.isIS_QUIET_MODE()) {
        LOGGER.debug("{} 查到数据点数: {}", Thread.currentThread().getName(), cnt);
      }
      return new Status(true, cnt);
    } catch (Exception e) {
      LOGGER.error("Failed send http result, because" + e);
      return new Status(false, e, e.getMessage());
    }
  }

  /**
   * add group by clause for query.
   *
   * @param sqlHeader sql header
   * @param timeGranularity time granularity of group by
   */
  @Override
  public String addGroupByClause(String sqlHeader, long timeGranularity) {
    return sqlHeader + " GROUP BY date_bin(INTERVAL '" + timeGranularity + "' MILLISECOND, time)";
  }

  /**
   * generate aggregation query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT count(s_0), count(s_3) FROM root.group_0, root.group_1
   *     WHERE(device='d_0' OR device='d_1')
   */
  @Override
  public String getAggQuerySqlHead(List<DeviceSchema> devices, String method) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    if (config.isALIGN_BY_DEVICE()) {
      builder.append("device, ");
    }
    List<Sensor> querySensors = devices.get(0).getSensors();

    if (Objects.equals(method, "last")) {
      builder.append(method).append("(time, ").append(querySensors.get(0).getName()).append(")");
    } else {
      builder.append(method).append("(").append(querySensors.get(0).getName()).append(")");
    }

    for (int i = 1; i < querySensors.size(); i++) {
      if (Objects.equals(method, "last")) {
        builder
            .append(", ")
            .append(method)
            .append("(time, ")
            .append(querySensors.get(i).getName())
            .append(")");
      } else {
        builder
            .append(", ")
            .append(method)
            .append("(")
            .append(querySensors.get(i).getName())
            .append(")");
      }
    }

    builder.append(super.generateConstrainForDevices(devices));
    return builder.toString();
  }

  /** Using in verification */
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

    StringBuilder buffer = this.buffer.get();
    if (buffer == null) {
      buffer = new StringBuilder();
      this.buffer.set(buffer);
    } else {
      buffer.setLength(0);
    }
    buffer.append("time IN (");
    for (Record record : records) {
      if (record.getRecordDataValue().size() != sensors.size()) {
        return new Status(
            false,
            new TsdbException("Record data schema mismatch"),
            String.format(
                "Expected record values: %d, but got %d",
                sensors.size(), record.getRecordDataValue().size()));
      }
      buffer.append(record.getTimestamp() * TIMESTAMP_TO_NANO).append(", ");
    }
    buffer.setLength(buffer.length() - 2);
    String condTimestamps = buffer.append(")").toString();

    HashSet<String> responseTimestamps = new HashSet<>(records.size());

    for (int columnIdx = 0; columnIdx < sensors.size(); columnIdx++) {
      // Instant sensorInstant = Instant.now();

      Sensor sensor = sensors.get(columnIdx);
      SensorType sensorType = sensor.getSensorType();

      buffer.setLength(0);
      buffer
          .append("SELECT CAST(time AS BIGINT) AS time, ")
          .append(sensor.getName())
          .append(" FROM ")
          .append(deviceSchema.getGroup())
          .append(" WHERE device ='")
          .append(deviceSchema.getDevice())
          .append("'")
          .append("AND ")
          .append(condTimestamps)
          .append(" ORDER BY time ASC");
      String sql = buffer.toString();

      try (CnosdbResponse response = connection.executeQuery(sql)) {
        if (!response.isOk()) {
          // LOGGER.error("Error: query failed, SQL: {}, error: {}", sql,
          // response.getErrorMessage());
          return new Status(false, response.getFailException(), response.getErrorMessage());
        }
        if (response.getResponseTitles() == null || response.getResponseTitles().length != 2) {
          String errorMessage =
              String.format(
                  "unexpected response titles, SQL: %s, titles: %s",
                  sql, StringUtils.join(response.getResponseTitles(), ", "));
          // LOGGER.error("Error: {}", errorMessage);
          return new Status(false, new TsdbException("unexpected result set"), errorMessage);
        }

        int lineNum = 0;
        String[] line;
        while (response.hasNext()) {
          Record r = records.get(lineNum);
          if (r == null) {
            String errorMessage =
                String.format(
                    "response is larger than expected, SQL: %s, expected: %s, response: %s",
                    sql, records.size(), lineNum);
            // LOGGER.error("Error: {}", errorMessage);
            return new Status(false, new TsdbException("unexpected result set"), errorMessage);
          }
          line = response.next();
          responseTimestamps.add(line[0]);
          long timestamp = r.getTimestamp() * TIMESTAMP_TO_NANO;
          if (!compareInt64WithString(timestamp, line[0])) {
            String errorMessage =
                String.format(
                    "response timestamp mismatch, SQL: %s, line: %s, expected: %s, response: %s",
                    sql, lineNum, timestamp, line[0]);
            // LOGGER.error("Error: {}", errorMessage);
            return new Status(false, new TsdbException("unexpected result set"), errorMessage);
          }
          Object value = r.getRecordDataValue().get(columnIdx);
          if (!compare(sensorType, value, line[1])) {
            String errorMessage =
                String.format(
                    "Error: response value mismatch, SQL: %s, line: %s, expected: %s, response: %s",
                    sql, lineNum, value, line[1]);
            // LOGGER.error("Error: {}", errorMessage);
            return new Status(false, new TsdbException("unexpected result set"), errorMessage);
          }
          lineNum += 1;
        }
      } catch (IOException e) {
        LOGGER.error("Error: query failed, because " + e);
        return new Status(false, e, e.getMessage());
      }
      // LOGGER.debug("Sensor costs: {} ms", Instant.now().toEpochMilli() -
      // sensorInstant.toEpochMilli());
    }

    LOGGER.debug("Total costs: {} ms", Instant.now().toEpochMilli() - startInstant.toEpochMilli());
    return new Status(true, responseTimestamps.size());
  }

  static boolean compare(SensorType dataType, Object dataValue, String resultValue) {
    switch (dataType) {
      case BOOLEAN:
        if (compareBooleanWithString((Boolean) dataValue, resultValue)) {
          return true;
        }
        break;
      case INT32:
        if (compareInt32WithString((Integer) dataValue, resultValue)) {
          return true;
        }
        break;
      case INT64:
        if (compareInt64WithString((Long) dataValue, resultValue)) {
          return true;
        }
        break;
      case FLOAT:
        if (compareFloatWithString((Float) dataValue, resultValue)) {
          return true;
        }
        break;
      case DOUBLE:
        if (compareDoubleWithString((Double) dataValue, resultValue)) {
          return true;
        }
        break;
      case TEXT:
        if (compareTextWithString((String) dataValue, resultValue)) {
          return true;
        }
        break;
      default:
        LOGGER.error(
            "Error Type: {}, record: '{}', response: '{}'", dataType, dataValue, resultValue);
        return false;
    }
    LOGGER.error(
        "Data not equal: Type: {}, record: '{}', response: '{}'", dataType, dataValue, resultValue);
    return false;
  }

  static boolean compareBooleanWithString(Boolean a, String b) {
    if (a == null) {
      return b == null;
    }
    if (a) {
      return b.equals("true");
    } else {
      return b.equals("false");
    }
  }

  static boolean compareInt32WithString(Integer a, String b) {
    if (a == null) {
      return b == null;
    }
    try {
      return a.equals(Integer.parseInt(b));
    } catch (NumberFormatException e) {
      return false;
    }
  }

  static boolean compareInt64WithString(Long a, String b) {
    if (a == null) {
      return b == null;
    }
    try {
      return a.equals(Long.parseLong(b));
    } catch (NumberFormatException e) {
      return false;
    }
  }

  static boolean compareFloatWithString(Float a, String b) {
    if (a == null) {
      return b == null;
    }
    try {
      float bv = Float.parseFloat(b);
      return Math.abs(a - bv) < 1E-9;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  static boolean compareDoubleWithString(Double a, String b) {
    if (a == null) {
      return b == null;
    }
    try {
      double bv = Double.parseDouble(b);
      return Math.abs(a - bv) < 1E-9;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  static boolean compareTextWithString(String a, String b) {
    if (a == null) {
      return b == null;
    }
    return a.equals(b);
  }
}
