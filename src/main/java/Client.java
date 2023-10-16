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


public class Client {
  private static final int THREAD_COUNT = 10;
  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: Client threadGroupSize numThreadGroups delay IPAddr");
      return;
    }

    int threadGroupSize = Integer.parseInt(args[0]);
    int numThreadGroups = Integer.parseInt(args[1]);
    int delay = Integer.parseInt(args[2]);
    String serverUri = args[3];
    int threadPoolSize = numThreadGroups * threadGroupSize; // Calculate the appropriate thread pool size

//    CloseableHttpClient httpClient = HttpClients.createDefault();
    ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

    try {

      long startTime = System.currentTimeMillis();
      CountDownLatch latch = new CountDownLatch(threadPoolSize);

      // make this for loop is another threat pool
      for (int group = 1; group <= numThreadGroups; group++) {
        System.out.println("Processing thread group: " + group);

        for (int i = 0; i < threadGroupSize; i++) {
          executorService.submit(() -> {
            try {
              performPostRequest(serverUri);
              System.out.println(Thread.currentThread().getName());
              performGetRequest(serverUri);
            } finally {
              latch.countDown();
            }
          });
        }
        Thread.sleep(delay * 1000); // Convert delay from seconds to milliseconds
      }

      executorService.shutdown();
      while (!executorService.isTerminated()) {
        System.out.println("ExecutorService is still active. Waiting for termination...");
        try {
          Thread.sleep(1000); // Wait for 1 second before checking again
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      System.out.println("ExecutorService has been shut down.");

      latch.await(); // Waiting for all tasks to complete

      long endTime = System.currentTimeMillis();
      long wallTime = (endTime - startTime) / 1000; // Convert to seconds
      long totalRequests = (long) numThreadGroups * threadGroupSize * 2000;
      double throughput = (double) totalRequests / wallTime;

      System.out.println("Wall Time: " + wallTime + " seconds");
      System.out.println("Throughput: " + throughput + " requests/second");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void performPostRequest( String serverUri) {
    //create client here and do the same thing at you get request
    HttpClient httpClient = HttpClients.createDefault();
    HttpPost postRequest = new HttpPost(serverUri + "/albums");
    // Configure the request if needed
    try {
      // Creating the form-data entity
      MultipartEntityBuilder builder = MultipartEntityBuilder.create();
      // Add the image file
      File imageFile = new File("nmtb.png");

      builder.addBinaryBody("image", imageFile);
      builder.addTextBody("profile",
          "{\"artist\":\"John Doe\",\"title\":\"Greatest Hits\",\"year\":\"2023\"}");

      HttpEntity multipart = builder.build();
      postRequest.setEntity(multipart);

      System.out.println("Executing POST request to: " + serverUri);

      HttpResponse response = httpClient.execute(postRequest);

      int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode == 200) {
        // Request was successful, do something
        System.out.println("Post request successful!");
      } else {
        // Request failed, handle the error
        System.out.println("Post request failed with status code: " + statusCode);
        // Get the response entity and log the content
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          String result = EntityUtils.toString(entity);
          System.out.println("Response content: " + result);
        }
      }

    } catch (Exception e) {
      System.err.println("An exception occurred during the POST request:");
      e.printStackTrace();
    }
  }

  private static void performGetRequest(String serverUri) {
    HttpClient httpClient = HttpClients.createDefault();
    HttpGet getRequest = new HttpGet(serverUri + "/albums/123");
    // Configure the request if needed
    try {
      HttpResponse response = httpClient.execute(getRequest);

      int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode == 200) {
        // Request was successful, do something
        System.out.println("GET request successful!");
        // Get the response entity and log the content
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          String result = EntityUtils.toString(entity);
          System.out.println("Response content: " + result);
        }
      } else {
        // Request failed, handle the error
        System.out.println("GET request failed with status code: " + statusCode);
        // Get the response entity and log the content
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          String result = EntityUtils.toString(entity);
          System.out.println("Response content: " + result);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
