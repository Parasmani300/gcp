package com.example.gcpdemo.service;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import com.google.common.base.Utf8;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class GCSOperations {

    private static final String SPLIT_PART_PREFIX = "part-";
    private static final int SPLIT_SIZE_BYTES = 1024 * 1024; // 1MB (adjust as needed)

    @Autowired
    Storage storage;

    public List<String> getFileNamesInBucketMod(String bucketName,String directoryPath)
    {
        List<String> fileNames = new ArrayList<>();
        Page<Blob> blobs = storage.list(bucketName,
                Storage.BlobListOption.prefix(directoryPath));

        for(Blob blob : blobs.iterateAll())
        {
            System.out.println(blob.getName() + ": " +  blob.getSize());
            long sourceSize = blob.getSize();

            InputStream inputStream = Channels.newInputStream(blob.reader());
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader  bufferedReader = new BufferedReader(inputStreamReader);

            try(BufferedReader reader = bufferedReader)
            {
                String line;
                int cnt = 0;
                List<String> linesInFile = new ArrayList<>();
                int k = 0;
//                GetHeader
                String headerLine = reader.readLine();
                linesInFile.add(headerLine);
                while((line = reader.readLine()) != null){
//                    System.out.println(line);
                    linesInFile.add(line);
                    if(cnt == 10000 ){
                        cnt = 0;
                        String listToString = linesInFile.stream().collect(Collectors.joining(System.lineSeparator()));
                        BlobInfo splitInfo =
                                BlobInfo.newBuilder(bucketName, blob.getName()+ SPLIT_PART_PREFIX + k + ".csv").build();
                        storage.create(splitInfo,listToString.getBytes());
                        k++;
                        linesInFile.clear();
                        linesInFile.add(headerLine);
                        fileNames.add(splitInfo.getName());
                        System.out.println("Created file: " + splitInfo.getName());
                    }
                    cnt++;
                }

                if(cnt != 0){
                    System.out.println(cnt);
                    cnt = 0;
                    String listToString = linesInFile.stream().collect(Collectors.joining(System.lineSeparator()));
                    BlobInfo splitInfo =
                            BlobInfo.newBuilder(bucketName, blob.getName()+ SPLIT_PART_PREFIX + k + ".csv").build();
                    storage.create(splitInfo,listToString.getBytes());
                    k++;
                    linesInFile.clear();
                    fileNames.add(splitInfo.getName());
                    System.out.println("Created file: " + splitInfo.getName());
                }
                storage.delete(blob.getBlobId());
            }catch (IOException ioException)
            {
                ioException.printStackTrace();
            }
        }

        return null;
    }

    public List<String> getFileNamesInBucket(String bucketName,String directorypath)
    {
        List<String> fileNames = new ArrayList<>();

//        for(Bucket bucket : storage.list().iterateAll()){
//            System.out.println(bucket);
//
//            for(Blob blob : bucket.list().iterateAll())
//            {
//                System.out.println(blob);
//            }
//        }
        Page<Blob> blobs = storage.list(bucketName,
                Storage.BlobListOption.prefix(directorypath));

        for(Blob blob : blobs.iterateAll()){
            System.out.println(blob.getName() + ": " +  blob.getSize());
            long sourceSize = blob.getSize();
            long numSplits = (sourceSize + SPLIT_SIZE_BYTES - 1) / SPLIT_SIZE_BYTES; // Round up

            byte[] content = blob.getContent();
            System.out.println(content);

            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(new ByteArrayInputStream(content))
            );
            if(blob.getSize() > 1024*1024) {
                try{
                    int count = 0;
                    int k = 0;
                    List<String> list = new ArrayList<>();
                    var line = bufferedReader.readLine();
                    while(line != null){
                        // System.out.println(line);
                        line = bufferedReader.readLine();
                        list.add(line);
                        if(count == 10000){
                            count = 0;
                            String listToString = list.stream().collect(Collectors.joining(System.lineSeparator()));
                            BlobInfo splitInfo =
                                                BlobInfo.newBuilder(bucketName, blob.getName()+ SPLIT_PART_PREFIX + k + ".csv").build();
                            storage.create(splitInfo,listToString.getBytes());
                            k++;
                            list.clear();
                            fileNames.add(splitInfo.getName());
                            System.out.println("Created " + splitInfo.getName());
                        }
                        count++;
                    }
                    storage.delete(blob.getBlobId());
                    System.out.println("deleted " + blob.getName());
                }catch (Exception ex){
                    ex.printStackTrace();
                }

            }else{
                fileNames.add(blob.getName());
            }
        }
        return  fileNames;
    }

}
