package ClientPart1;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
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


public class Client {
  private static final int THREAD_COUNT = 10;
  private static final AtomicInteger atomicInteger = new AtomicInteger(0);

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
                for(int k = 0; k < 1000; k++){
                  atomicInteger.addAndGet(performPostRequest(serverUri));
                  atomicInteger.addAndGet(performGetRequest(serverUri));
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
      long wallTime = (endTime - startTime) / 1000; // Convert to seconds
      double throughput = (double) atomicInteger.get() / wallTime;


      System.out.println("Wall Time: " + wallTime + " seconds");
      System.out.println("Throughput: " + throughput + " requests/second");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static int performPostRequest( String serverUri) {
    int requestCount = 1; // Keep track of the number of requests

    //create client here and do the same thing at you get request
    HttpClient httpClient = HttpClients.createDefault();
    HttpPost postRequest = new HttpPost(serverUri + "/albums");
    // Configure the request if needed
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

    } catch (Exception e) {
      e.printStackTrace();
    }
    return requestCount;

  }

  private static int performGetRequest(String serverUri) {
    int requestCount = 1; // Keep track of the number of requests

    HttpClient httpClient = HttpClients.createDefault();
    HttpGet getRequest = new HttpGet(serverUri + "/albums/123");
    // Configure the request if needed
    try {
      HttpResponse response = httpClient.execute(getRequest);
      int statusCode = response.getStatusLine().getStatusCode();

      if (!(statusCode == 200 ||statusCode == 201)) {
        HttpEntity entity = response.getEntity();
        requestCount++;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return requestCount;

  }

}
