import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import java.io.File;
import java.util.concurrent.Executors;
import org.apache.http.util.EntityUtils;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.util.concurrent.atomic.AtomicInteger;

public class Client2 {
  private static final int THREAD_COUNT = 10;
  private static LinkedBlockingQueue<RequestDetail> requestDetails = new LinkedBlockingQueue<>();
  private static final AtomicInteger atomicInteger = new AtomicInteger(0);

  public static void main(String[] args) {
    if (args.length != 4) {
      return;
    }

    int threadGroupSize = Integer.parseInt(args[0]);
    int numThreadGroups = Integer.parseInt(args[1]);
    int delay = Integer.parseInt(args[2]);
    String serverUri = args[3];
    int threadPoolSize = numThreadGroups * threadGroupSize; // Calculate the appropriate thread pool size

    ExecutorService executorService1 = Executors.newFixedThreadPool(threadPoolSize);
    ExecutorService executorService2 = Executors.newFixedThreadPool(threadPoolSize);

    try {

      long startTime = System.currentTimeMillis();
      CountDownLatch latch = new CountDownLatch(threadPoolSize);

      for (int group = 1; group <= numThreadGroups; group++) {
        executorService1.submit(() -> {
          for (int i = 0; i < threadGroupSize; i++) {
            executorService2.submit(() -> {
              try {
                for(int k = 0; k < 10; k++){
                  atomicInteger.addAndGet(performPostRequest(serverUri));
                  atomicInteger.addAndGet(performGetRequest(serverUri));
                }
              } finally {
                latch.countDown();
              }
            });
          }
        });
        executorService2.shutdown();
        Thread.sleep(delay * 1000); // Convert delay from seconds to milliseconds
        while (!executorService2.isTerminated()) {
          try{
            Thread.sleep(1000);
          }catch(InterruptedException e){
            e.printStackTrace();
          }
        }
      }
      System.out.println("checkpoint");

      executorService1.shutdown();
      while (!executorService1.isTerminated()) {
        try{
          Thread.sleep(1000);
        }catch(InterruptedException e){
          e.printStackTrace();
        }
      }
      System.out.println("checkpoint2");

//      System.out.println(atomicInteger);
      latch.await(); // Waiting for all tasks to complete

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
      // Calculate response time statistics
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

  private static int performPostRequest( String serverUri) {
    int requestCount = 1; // Keep track of the number of requests

    long startTime = System.currentTimeMillis(); // Capture start time

    HttpClient httpClient = HttpClients.createDefault();
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

  private static int performGetRequest(String serverUri) {
    int requestCount = 1; // Keep track of the number of requests

    long startTime = System.currentTimeMillis(); // Capture start time

    HttpClient httpClient = HttpClients.createDefault();
    HttpGet getRequest = new HttpGet(serverUri + "/albums/123");

    try {
      HttpResponse response = httpClient.execute(getRequest);
      int statusCode = response.getStatusLine().getStatusCode();

      if (!(statusCode == 200 ||statusCode == 201)) {
        HttpEntity entity = response.getEntity();
        requestCount++;
//        if (entity != null) {
//          String result = EntityUtils.toString(entity);
//        }
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
