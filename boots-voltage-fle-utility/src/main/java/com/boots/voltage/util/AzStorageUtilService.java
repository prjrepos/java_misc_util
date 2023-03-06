package main.java.com.boots.voltage.util;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;


public class AzStorageUtilService {

    static public DataLakeServiceClient GetDataLakeServiceClientByAccountKey(String accountName, String accountKey) {

        StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
        builder.credential(sharedKeyCredential);
        // builder.endpoint("https://" + accountName + ".blob.core.windows.net");
        builder.endpoint("https://" + accountName + ".dfs.core.windows.net");
        return builder.buildClient();
    }

    static public DataLakeServiceClient GetDataLakeServiceClientByAAD(String accountName, String clientId,
            String ClientSecret, String tenantID) {

        // String endpoint = "https://" + accountName + ".blob.core.windows.net";
        String endpoint = "https://" + accountName + ".dfs.core.windows.net";

        ClientSecretCredential cred = new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .clientSecret(ClientSecret)
                .tenantId(tenantID)
                .build();

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
        builder.credential(cred);
        builder.endpoint(endpoint);
        return builder.buildClient();
    }
    
    

}
