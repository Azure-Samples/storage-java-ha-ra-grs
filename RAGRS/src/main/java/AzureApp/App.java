package AzureApp;// MIT License
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE


import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.azure.storage.blob.options.BlobDownloadToFileOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Random;

public class App {

    public static final String storageConnectionString = System.getenv("DefaultEndpointsProtocol=https;AccountName=409450forlh;AccountKey=KQr9H+LkizGOH/HaVhIIiXzce1gxdDHg1oOshDd/Az8kVZALZ+jjx1C8n4xzQzz//688TYnTWJZQ05LuqxiPhA==;EndpointSuffix=core.windows.net");

    public static void main(String[] args) {
        File sourceFile = null, downloadedFile = null;
        System.out.println("Azure Blob storage quick start sample");

        BlobContainerClient blobContainerClient = null;

        Random rand = new Random();

        final int maxAttempts = 10;

        try {
            // Parse the connection string and create a blob client to interact with Blob storage
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(storageConnectionString).buildClient();
            // Create the container if it does not exist with public access.
            blobContainerClient = blobServiceClient.getBlobContainerClient("quickstartcontainer" + rand.nextInt());
            if (!blobContainerClient.exists()) {
                blobContainerClient.create();
            }
            System.out.println("Creating container: " + blobContainerClient.getBlobContainerName());

            //Creating a sample file
            sourceFile = File.createTempFile("sampleFile", ".txt");
            System.out.println("Creating a sample file at: " + sourceFile.toString());
            Writer output = new BufferedWriter(new FileWriter(sourceFile));
            output.write("Hello Azure!");
            output.close();

            //Getting a blob reference
            BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(sourceFile.getName()).getBlockBlobClient();

            //Creating blob and uploading file to it
            System.out.println("Uploading the sample file ");
            blockBlobClient.upload(new FileInputStream(sourceFile), sourceFile.length());

            int counter = 0;
            while (counter < 60) {
                counter++;

                System.out.println("Attempt " + counter + " to see if the blob has replicated to secondary yet.");

                if (blockBlobClient.exists()) {
                    break;
                }

                Thread.sleep(1000);
            }

            for (int i = 0; i < 1000; i++) {
                if (i == 200 || i == 400 || i == 600 || i == 800) {
                    System.out.println("Press enter to continue.");
                    System.in.read();
                }
                try {

                    // Download blob. In most cases, you would have to retrieve the reference
                    // to cloudBlockBlob here. However, we created that reference earlier, and
                    // haven't changed the blob we're interested in, so we can reuse it.
                    // Here we are creating a new file to download to. Alternatively you can also pass in the path as a string into downloadToFile method: blob.downloadToFile("/path/to/new/file").
                    downloadedFile = new File(sourceFile.getParentFile(), "downloadedFile.txt");
                    BlobDownloadToFileOptions blobDownloadToFileOptions
                            = new BlobDownloadToFileOptions(downloadedFile.getAbsolutePath())
                            .setDownloadRetryOptions(new DownloadRetryOptions().setMaxRetryRequests(maxAttempts));
                    blockBlobClient.downloadToFileWithResponse(
                            blobDownloadToFileOptions, null, null).getValue();
                    FileInputStream fis = new FileInputStream(downloadedFile);
                    InputStream input = new BufferedInputStream((fis));
                    byte[] bytesArray = new byte[(int) downloadedFile.length()];
                    input.read(bytesArray);
                    input.close();
                    fis.close();

                    System.out.print("The contents of the downloaded file are: ");
                    System.out.write(bytesArray);
                    System.out.println();
                    System.out.println("The blob has been downloaded from: " + blockBlobClient.getBlobUrl());
                } catch (Exception ex) {
                    System.out.println(ex.toString());
                }
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            System.out.println("The program has completed successfully.");
            System.out.println("Press the 'Enter' key while in the console to delete the sample files, example container, and exit the application.");

            //Pausing for input
            try {
                System.in.read();
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }

            System.out.println("Deleting the container");
            if(blobContainerClient.exists()){
                blobContainerClient.delete();
            }

            System.out.println("Deleting the source and downloaded files");

            if (downloadedFile != null)
                downloadedFile.deleteOnExit();

            if (sourceFile != null)
                sourceFile.deleteOnExit();
        }
    }
}
