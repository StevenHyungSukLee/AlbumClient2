package ClientPart2;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


public class Client2 {

  private static LinkedBlockingQueue<RequestDetail> getRequestsDetails = new LinkedBlockingQueue<>();
  private static LinkedBlockingQueue<RequestDetail> postRequestsDetails = new LinkedBlockingQueue<>();

  private static final AtomicInteger atomicInteger = new AtomicInteger(0);
  private static final AtomicInteger successfulRequests = new AtomicInteger(0);
  private static final AtomicInteger failedRequests = new AtomicInteger(0);
  private static final AtomicReference<String> lastAlbumId = new AtomicReference<>(
      ""); // Changed to AtomicReference for String type

  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: ClientPart1.Client threadGroupSize numThreadGroups delay IPAddr");
      return;
    }

    int threadGroupSize = Integer.parseInt(args[0]);
    int numThreadGroups = Integer.parseInt(args[1]);
    int delay = Integer.parseInt(args[2]);
    String serverUri = args[3];
    int threadPoolSize =
        numThreadGroups * threadGroupSize; // Calculate the appropriate thread pool size

    long startTime = System.currentTimeMillis();

    try {
      ExecutorService groupExecutorService = Executors.newFixedThreadPool(numThreadGroups);
      CountDownLatch groupLatch = new CountDownLatch(numThreadGroups);

      for (int group = 0; group < numThreadGroups; group++) {
        final int currentGroup = group;

//        CountDownLatch latch = new CountDownLatch(threadGroupSize);
        groupExecutorService.submit(() -> {
          try {
            ExecutorService threadExecutorService = Executors.newFixedThreadPool(threadGroupSize);
            CountDownLatch threadLatch = new CountDownLatch(threadGroupSize);
            for (int i = 0; i < threadGroupSize; i++) {
              threadExecutorService.submit(() -> {
                System.out.println(Thread.currentThread().getName());
                HttpClient httpClient = HttpClients.createDefault();
                for (int k = 0; k < 1000; k++) {
                  atomicInteger.addAndGet(performPostRequest(serverUri, httpClient));
                  atomicInteger.addAndGet(performGetRequest(serverUri, httpClient));
                }
                threadLatch.countDown();
              });
            }
            threadExecutorService.shutdown();
            threadLatch.await(); // Wait for all threads in group to finish

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            groupLatch.countDown();
          }
        });
        if (currentGroup < numThreadGroups - 1) {
          try {
            Thread.sleep(delay * 1000); // Delay before starting next group
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
      groupExecutorService.shutdown();
      groupLatch.await(); // Wait for all thread groups to finis
      long endTime = System.currentTimeMillis();

      List<Long> latencies = new ArrayList<>();
      List<Long> getLatencies = new ArrayList<>();
      List<Long> postLatencies = new ArrayList<>();
//      writeRecordsToCSV(requestDetails, "statistics.csv");
      for (RequestDetail detail : getRequestsDetails) {
        getLatencies.add(detail.getLatency());
      }
      for (RequestDetail detail : postRequestsDetails) {
        postLatencies.add(detail.getLatency());
      }
      long wallTime = (endTime - startTime) / 1000; // Convert to seconds
      double throughput = (double) atomicInteger.get() / wallTime;

      System.out.println("Wall Time: " + wallTime + " seconds");
      System.out.println("Throughput: " + throughput + " requests/second");

      // Calculate statistics for GET requests
      long getMeanResponseTime = getMean(getLatencies);
      long getMedianResponseTime = getMedian(getLatencies);
      long getP99GetResponseTime = getPercentile(getLatencies, 99);
      long getMinResponseTime = Collections.min(getLatencies);
      long getMaxResponseTime = Collections.max(getLatencies);

      // Calculate statistics for POST requests
      long postMeanResponseTime = getMean(postLatencies);
      long postMedianResponseTime = getMedian(postLatencies);
      long getP99PostResponseTime = getPercentile(postLatencies, 99);
      long postMinResponseTime = Collections.min(postLatencies);
      long postMaxResponseTime = Collections.max(postLatencies);

      System.out.println("GET Mean Response Time: " + getMeanResponseTime + " milliseconds");
      System.out.println("GET Median Response Time: " + getMedianResponseTime + " milliseconds");
      System.out.println("GET P99 Response Time: " + getP99GetResponseTime + " milliseconds");
      System.out.println("GET Min Response Time: " + getMinResponseTime + " milliseconds");
      System.out.println("GET Max Response Time: " + getMaxResponseTime + " milliseconds");

      System.out.println("POST Mean Response Time: " + postMeanResponseTime + " milliseconds");
      System.out.println("POST Median Response Time: " + postMedianResponseTime + " milliseconds");
      System.out.println("POST P99 Response Time: " + getP99PostResponseTime + " milliseconds");
      System.out.println("POST Min Response Time: " + postMinResponseTime + " milliseconds");
      System.out.println("POST Max Response Time: " + postMaxResponseTime + " milliseconds");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static int performPostRequest(String serverUri, HttpClient httpClient) {
    int requestCount = 1; // Keep track of the number of requests
    int retryAttempts = 0; // Counter for retry attempts

    long startTime = System.currentTimeMillis(); // Capture start time

    while (retryAttempts < 5) { // Retry up to 5 times

      HttpPost postRequest = new HttpPost(serverUri + "/albums");

      try {
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        File imageFile = new File("nmtb.png");

        builder.addBinaryBody("image", imageFile);
        builder.addTextBody("artist", "John Doe");
        builder.addTextBody("title", "Greatest Hits");
        builder.addTextBody("year", "2023");

        HttpEntity multipart = builder.build();
        postRequest.setEntity(multipart);

        HttpResponse response = httpClient.execute(postRequest);
        int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode == 200 || statusCode == 201) {
          // If successful, exit the loop
          HttpEntity entity = response.getEntity();
          String responseBody = EntityUtils.toString(entity);
          JsonObject responseJson = JsonParser.parseString(responseBody).getAsJsonObject();
          if (responseJson.has("albumId")) {
            lastAlbumId.set(responseJson.get("albumId").getAsString()); // Store the new album ID
          }
          EntityUtils.consume(entity);
          EntityUtils.consume(response.getEntity());

          successfulRequests.incrementAndGet(); // Increment successful request counter
          long endTime = System.currentTimeMillis(); // Capture end time
          long latency = endTime - startTime; // Calculate latency
          postRequestsDetails.add(new RequestDetail(startTime, "POST", latency, statusCode));

          break;
        } else if (statusCode >= 400 && statusCode <= 599) {
          // Increment retryAttempts if 4XX or 5XX response code
          if (retryAttempts == 4) {
            failedRequests.incrementAndGet();
          }
          retryAttempts++;
          requestCount++;
        }

      } catch (Exception e) {
        if (retryAttempts == 4) {
          failedRequests.incrementAndGet();
        }
        e.printStackTrace();
        retryAttempts++;

      }
    }
    return requestCount;
  }

  private static int performGetRequest(String serverUri, HttpClient httpClient) {
    int requestCount = 1; // Keep track of the number of requests
    int retryAttempts = 0; // Counter for retry attempts

    long startTime = System.currentTimeMillis(); // Capture start time

    while (retryAttempts < 5) { // Retry up to 5 times
      String currentAlbumId = lastAlbumId.get();
      if (currentAlbumId.isEmpty()) {
        System.err.println("No valid album ID available for GET request.");
        break; // Skip the GET request if no valid ID is available
      }
      HttpGet getRequest = new HttpGet(serverUri + "/albums/" + currentAlbumId);

      try {
        HttpResponse response = httpClient.execute(getRequest);
        EntityUtils.consume(response.getEntity());

        int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode == 200 || statusCode == 201) {
          successfulRequests.incrementAndGet(); // Increment successful request counter
          long endTime = System.currentTimeMillis(); // Capture end time
          long latency = endTime - startTime; // Calculate latency
          getRequestsDetails.add(new RequestDetail(startTime, "GET", latency, statusCode));

          break;
        } else if (statusCode >= 400 && statusCode <= 599) {
          // Increment retryAttempts if 4XX or 5XX response code
          if (retryAttempts == 4) {
            failedRequests.incrementAndGet();
          }
          retryAttempts++;
          requestCount++;
        }

      } catch (Exception e) {
        if (retryAttempts == 4) {
          failedRequests.incrementAndGet();
        }
        e.printStackTrace();
        retryAttempts++; // Increment retry attempts on exception

      }
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
}