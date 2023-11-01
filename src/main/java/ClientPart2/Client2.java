package ClientPart2;

import com.opencsv.CSVWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class Client2 {
  private static final int THREAD_COUNT = 10;
  private static LinkedBlockingQueue<RequestDetail> requestDetails = new LinkedBlockingQueue<>();
  private static final AtomicInteger atomicInteger = new AtomicInteger(0);
  private static final HttpClient httpClient = HttpClients.createDefault();

  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: ClientPart1.Client threadGroupSize numThreadGroups delay IPAddr");
      return;
    }

    int threadGroupSize = Integer.parseInt(args[0]);
    int numThreadGroups = Integer.parseInt(args[1]);
    int delay = Integer.parseInt(args[2]);
    String serverUri = args[3];

    int threadPoolSize = numThreadGroups * threadGroupSize; // Calculate the appropriate thread pool size

    ExecutorService executorService1 = Executors.newFixedThreadPool(threadPoolSize);
    try {

      long startTime = System.currentTimeMillis();
      CountDownLatch latch = new CountDownLatch(threadPoolSize);

      for (int group = 1; group <= numThreadGroups; group++) {
        executorService1.submit(() -> {
          ExecutorService executorService2 = Executors.newFixedThreadPool(threadPoolSize);
          for (int i = 0; i < threadGroupSize; i++) {
            executorService2.submit(() -> {
              try {
                HttpClient httpClient = HttpClients.createDefault();
                for(int k = 0; k < 1000; k++){
                  atomicInteger.addAndGet(performPostRequest(serverUri, httpClient));
                  atomicInteger.addAndGet(performGetRequest(serverUri, httpClient));
                }
              } finally {
                latch.countDown();
              }
            });
          }
          executorService2.shutdown();
          try {
            Thread.sleep(delay * 1000); // Convert delay from seconds to milliseconds
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          while (!executorService2.isTerminated()) {
          }
        });
      }

      executorService1.shutdown();
      latch.await();
      while (!executorService1.isTerminated()) {
      }

      long endTime = System.currentTimeMillis();

      List<Long> latencies = new ArrayList<>();
      writeRecordsToCSV(requestDetails, "statistics.csv");
      for (int i = 0; i < requestDetails.size(); i++) {
        RequestDetail requestDetail = requestDetails.poll();
        if (requestDetail != null) {
          latencies.add(requestDetail.getLatency());
        }
      }


      long wallTime = (endTime - startTime) / 1000; // Convert to seconds
      double throughput = (double) atomicInteger.get() / wallTime;

      System.out.println("Wall Time: " + wallTime + " seconds");
      System.out.println("Throughput: " + throughput + " requests/second");
      long meanResponseTime = getMean(latencies);
      long medianResponseTime = getMedian(latencies);
      long p99ResponseTime = getPercentile(latencies, 99);
      long minResponseTime = Collections.min(latencies);
      long maxResponseTime = Collections.max(latencies);

      // Display response time statistics
      System.out.println("Mean Response Time: " + meanResponseTime + " milliseconds");
      System.out.println("Median Response Time: " + medianResponseTime + " milliseconds");
      System.out.println("P99 Response Time: " + p99ResponseTime + " milliseconds");
      System.out.println("Min Response Time: " + minResponseTime + " milliseconds");
      System.out.println("Max Response Time: " + maxResponseTime + " milliseconds");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static int performPostRequest(String serverUri, HttpClient httpClient) {
    int requestCount = 1; // Keep track of the number of requests

    long startTime = System.currentTimeMillis(); // Capture start time

    HttpPost postRequest = new HttpPost(serverUri + "/albums");

    try {
      MultipartEntityBuilder builder = MultipartEntityBuilder.create();
      File imageFile = new File("nmtb.png");

      builder.addBinaryBody("image", imageFile);
      builder.addTextBody("profile",
          "{\"artist\":\"John Doe\",\"title\":\"Greatest Hits\",\"year\":\"2023\"}");

      HttpEntity multipart = builder.build();
      postRequest.setEntity(multipart);
      HttpResponse response = httpClient.execute(postRequest);

      int statusCode = response.getStatusLine().getStatusCode();

      if (!(statusCode == 200 ||statusCode == 201)) {
        requestCount++;
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          String result = EntityUtils.toString(entity);
        }
      }

      long endTime = System.currentTimeMillis(); // Capture end time
      long latency = endTime - startTime; // Calculate latency
      requestDetails.add(new RequestDetail(startTime, "POST", latency, statusCode)); // Add request details to the queue

    } catch (Exception e) {
      e.printStackTrace();
    }
    return requestCount;
  }

  private static int performGetRequest(String serverUri, HttpClient httpClient) {
    int requestCount = 1; // Keep track of the number of requests

    long startTime = System.currentTimeMillis(); // Capture start time

    HttpGet getRequest = new HttpGet(serverUri + "/albums/123");

    try {
      HttpResponse response = httpClient.execute(getRequest);
      int statusCode = response.getStatusLine().getStatusCode();

      if (!(statusCode == 200 ||statusCode == 201)) {
        HttpEntity entity = response.getEntity();
        requestCount++;
      }

      long endTime = System.currentTimeMillis(); // Capture end time
      long latency = endTime - startTime; // Calculate latency
      requestDetails.add(new RequestDetail(startTime, "GET", latency, statusCode)); // Add request details to the queue

    } catch (Exception e) {
      e.printStackTrace();
    }

    return requestCount;
  }

  private static long getMean(List<Long> values) {
    long sum = 0;
    for (Long value : values) {
      sum += value;
    }
    return values.isEmpty() ? 0 : sum / values.size();
  }

  private static long getMedian(List<Long> values) {
    Collections.sort(values);
    int middle = values.size() / 2;
    if (values.size() % 2 == 1) {
      return values.get(middle);
    } else {
      return (values.get(middle - 1) + values.get(middle)) / 2;
    }
  }

  private static long getPercentile(List<Long> values, int percentile) {
    Collections.sort(values);
    int index = (int) Math.ceil(percentile / 100.0 * values.size());
    return values.get(index - 1);
  }

  private static class RequestDetail {
    private final long startTime;
    private final String requestType;
    private final long latency;
    private final int statusCode;

    public RequestDetail(long startTime, String requestType, long latency, int statusCode) {
      this.startTime = startTime;
      this.requestType = requestType;
      this.latency = latency;
      this.statusCode = statusCode;
    }

    public long getLatency() {
      return latency;
    }
  }
  private static void writeRecordsToCSV(LinkedBlockingQueue<RequestDetail> requestDetails, String filename) {
    try {
      CSVWriter writer = new CSVWriter(new FileWriter(filename));

      // Adding header to the CSV
      String[] header = {"Start Time", "Request Type", "Latency", "Response Code"};
      writer.writeNext(header);

      for (RequestDetail requestDetail : requestDetails) {
        String[] record = {
            String.valueOf(requestDetail.startTime),
            requestDetail.requestType,
            String.valueOf(requestDetail.latency),
            String.valueOf(requestDetail.statusCode)
        };
        writer.writeNext(record);
      }

      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}