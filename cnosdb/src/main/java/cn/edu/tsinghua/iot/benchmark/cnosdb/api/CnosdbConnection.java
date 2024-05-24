package cn.edu.tsinghua.iot.benchmark.cnosdb.api;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import okhttp3.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.TimeUnit;

public class CnosdbConnection {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final MediaType mediaTypeNdJson = MediaType.parse("application/nd-json");
  private static final MediaType mediaTypeCsv = MediaType.parse("application/csv");
  private final String url;
  private final String authorization;

  OkHttpClient ddlClient;
  OkHttpClient queryClient;

  public CnosdbConnection(
      String cnosdbUrl, String cnosdbUser, String cnosdbPassword, String cnosdbDatabase)
      throws MalformedURLException {
    this.ddlClient =
        new OkHttpClient()
            .newBuilder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.SECONDS)
            .writeTimeout(3, TimeUnit.SECONDS)
            .build();
    ConnectionPool connectionPool =
        new ConnectionPool(config.getCLIENT_NUMBER(), 5, TimeUnit.MINUTES);
    this.queryClient = new OkHttpClient().newBuilder().connectionPool(connectionPool).build();
    this.url = cnosdbUrl + "/api/v1/sql?db=" + cnosdbDatabase;
    this.authorization = Credentials.basic(cnosdbUser, cnosdbPassword);
  }

  public CnosdbResponse executeDdl(String sql) throws IOException {
    RequestBody requestBody = RequestBody.create(mediaTypeNdJson, sql.getBytes());
    Request request =
        new Request.Builder()
            .url(url)
            .addHeader("Accept", "application/nd-json")
            .addHeader("Authorization", this.authorization)
            .post(requestBody)
            .build();
    return new CnosdbResponse(ddlClient.newCall(request), false);
  }

  public CnosdbResponse executeQuery(String sql) {
    RequestBody requestBody = RequestBody.create(mediaTypeCsv, sql.getBytes());
    Request request =
        new Request.Builder()
            .url(url)
            .addHeader("Accept", "application/csv")
            .addHeader("Authorization", this.authorization)
            .post(requestBody)
            .build();
    return new CnosdbResponse(queryClient.newCall(request), true);
  }

  public void close() throws Exception {
    this.queryClient.dispatcher().executorService().shutdown();
  }
}
