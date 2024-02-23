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

  OkHttpClient client;

  public CnosdbConnection(
      String cnosdbUrl, String cnosdbUser, String cnosdbPassword, String cnosdbDatabase)
      throws MalformedURLException {
    ConnectionPool connectionPool =
        new ConnectionPool(config.getCLIENT_NUMBER(), 5, TimeUnit.MINUTES);
    this.client = new OkHttpClient().newBuilder().connectionPool(connectionPool).build();
    this.url = cnosdbUrl + "/api/v1/sql?db=" + cnosdbDatabase;
    this.authorization = Credentials.basic(cnosdbUser, cnosdbPassword);
  }

  public Response execute(String sql) throws IOException {
    RequestBody requestBody = RequestBody.create(mediaTypeNdJson, sql.getBytes());
    Request request =
        new Request.Builder()
            .url(url)
            .addHeader("Accept", "application/nd-json")
            .addHeader("Authorization", this.authorization)
            .post(requestBody)
            .build();
    return client.newCall(request).execute();
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
    return new CnosdbResponse(client.newCall(request));
  }
}
