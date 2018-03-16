import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

import java.io.*;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Date;
import java.util.Scanner;
import java.util.Random;

public class App {

    public static final String storageConnectionString = System.getenv("storageconnectionstring");

    public static void main( String[] args )
    {
        File sourceFile = null, downloadedFile = null;
        System.out.println("Azure Blob storage quick start sample");

        CloudStorageAccount storageAccount;
        CloudBlobClient blobClient = null;
        CloudBlobContainer container=null;
        Random rand = new Random();
        //rand.nextInt();

        try {
            // Parse the connection string and create a blob client to interact with Blob storage
            storageAccount = CloudStorageAccount.parse(storageConnectionString);
            blobClient = storageAccount.createCloudBlobClient();
            container = blobClient.getContainerReference("quickstartcontainer" + rand.nextInt());

            // Create the container if it does not exist with public access.
            System.out.println("Creating container: " + container.getName());
            container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());

            //Creating a sample file
            sourceFile = File.createTempFile("sampleFile", ".txt");
            System.out.println("Creating a sample file at: " + sourceFile.toString());
            Writer output = new BufferedWriter(new FileWriter(sourceFile));
            output.write("Hello Azure!");
            output.close();

            //Getting a blob reference
            CloudBlockBlob blob = container.getBlockBlobReference(sourceFile.getName());

            //Creating blob and uploading file to it
            System.out.println("Uploading the sample file ");
            blob.uploadFromFile(sourceFile.getAbsolutePath());

            BlobRequestOptions myReqOptions = new BlobRequestOptions();
            myReqOptions.setRetryPolicyFactory(new RetryLinearRetry(20,20));
            blobClient.setDefaultRequestOptions(myReqOptions);

            blobClient.getDefaultRequestOptions().setLocationMode(LocationMode.SECONDARY_ONLY);

            int counter = 0;
            while(counter < 60)
            {
                counter++;

                System.out.println("Attempt " + counter + " to see if the blob has replicated to secondary yet.");

                if(blob.exists())
                {
                    break;
                }

                Thread.sleep(1000);
            }

            blobClient.getDefaultRequestOptions().setLocationMode(LocationMode.PRIMARY_THEN_SECONDARY);
            OperationContext opContext = new OperationContext();

            for (int i = 0; i <1000; i++)
            {
                if(i == 200 || i == 400 || i == 600 || i == 800)
                {
                    System.out.println("Press enter to continue.");
                    System.in.read();
                }
                try{

                    // Download blob. In most cases, you would have to retrieve the reference
                    // to cloudBlockBlob here. However, we created that reference earlier, and
                    // haven't changed the blob we're interested in, so we can reuse it.
                    // Here we are creating a new file to download to. Alternatively you can also pass in the path as a string into downloadToFile method: blob.downloadToFile("/path/to/new/file").
                    downloadedFile = new File(sourceFile.getParentFile(), "downloadedFile.txt");
                    blob.downloadToFile(downloadedFile.getAbsolutePath(),null,blobClient.getDefaultRequestOptions(),opContext);
                    FileInputStream fis = new FileInputStream(downloadedFile);
                    InputStream input = new BufferedInputStream((fis));
                    byte[] bytesArray = new byte[(int) downloadedFile.length()];
                    input.read(bytesArray);
                    input.close();
                    fis.close();

                    System.out.print("The contents of the downloaded file are: " );
                    System.out.write(bytesArray);
                    System.out.println();
                    System.out.println("The blob has been downloaded from: " + opContext.getLastResult().getTargetLocation());
                }
                catch (Exception ex)
                {
                    System.out.println(ex.toString());
                }
            }
        }
        catch (StorageException ex)
        {
            System.out.println(String.format("Error returned from the service. Http code: %d and error code: %s", ex.getHttpStatusCode(), ex.getErrorCode()));
        }
        catch (InvalidKeyException ex)
        {
            System.out.println("Make sure your storageconnectionstring environment variable is set correctly and is accurate.");
        }
        catch (URISyntaxException ex)
        {
            System.out.println("Please make sure all URIs in your storageconnectionstring envrionement variable are valid.");
        }
        catch (Exception ex)
        {
            System.out.println(ex.getMessage());
        }
        finally
        {
            System.out.println("The program has completed successfully.");
            System.out.println("Press the 'Enter' key while in the console to delete the sample files, example container, and exit the application.");

            //Pausing for input
            Scanner sc = new Scanner(System.in);
            sc.nextLine();

            System.out.println("Deleting the container");
            try {
                if(container != null)
                    container.deleteIfExists();
            }
            catch (StorageException ex) {
                System.out.println(String.format("Service error. Http code: %d and error code: %s", ex.getHttpStatusCode(), ex.getErrorCode()));
            }

            System.out.println("Deleting the source, and downloaded files");

            if(downloadedFile != null)
                downloadedFile.deleteOnExit();

            if(sourceFile != null)
                sourceFile.deleteOnExit();

            //Closing scanner
            sc.close();
        }
    }
}
