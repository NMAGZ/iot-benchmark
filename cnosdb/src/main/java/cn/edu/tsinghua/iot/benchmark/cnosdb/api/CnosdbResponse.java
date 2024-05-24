package cn.edu.tsinghua.iot.benchmark.cnosdb.api;

import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import okhttp3.Call;
import okhttp3.Response;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

public class CnosdbResponse implements Iterator<String[]>, Closeable {
  private Response response;
  private boolean closed;
  private boolean ok;
  private String errorMessage;
  private Exception failException;
  private CSVReader resultReader;
  private Iterator<String[]> responseIterator;
  private String[] responseTitles;

  public CnosdbResponse(Call call, boolean isCsvResponse) {
    try {
      this.response = call.execute();
      if (response.body() != null) {
        if (response.code() / 100 == 2) {
          this.closed = false;
          this.ok = true;
          if (isCsvResponse) {
            this.resultReader =
                new CSVReaderBuilder(new InputStreamReader(response.body().byteStream())).build();
            this.responseIterator = this.resultReader.iterator();
            this.responseTitles = this.responseIterator.next();
          } else {
            this.closed = true;
          }
        } else {
          this.closed = true;
          this.ok = false;
          this.errorMessage = response.body().string();
          this.failException = new TsdbException(this.errorMessage);
          this.response.close();
        }
      }
    } catch (IOException e) {
      if (this.response != null) {
        this.response.close();
      }
      this.closed = true;
      this.ok = false;
      this.errorMessage = e.getMessage();
      this.failException = e;
    }
  }

  public int getCode() {
    return this.response.code();
  }

  public boolean isOk() {
    return this.ok;
  }

  public Exception getFailException() {
    return this.failException;
  }

  public String getErrorMessage() {
    return this.errorMessage;
  }

  public String[] getResponseTitles() {
    return this.responseTitles;
  }

  @Override
  public boolean hasNext() {
    if (this.closed) {
      return false;
    }
    return this.responseIterator.hasNext();
  }

  @Override
  public String[] next() {
    return this.responseIterator.next();
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    this.resultReader.close();
  }
}
