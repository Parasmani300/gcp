package com.example.gcpdemo.service;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import com.google.common.base.Utf8;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class GCSOperations {

    private static final String SPLIT_PART_PREFIX = "part-";
    private static final int SPLIT_SIZE_BYTES = 1024 * 1024; // 1MB (adjust as needed)

    @Autowired
    Storage storage;

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
