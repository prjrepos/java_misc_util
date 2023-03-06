package main.java.com.boots.voltage.util;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;

public class AzureKVUtil {

	public String getSecretValue(String secretName) throws Exception {
		String keyVaultName = System.getenv("KEY_VAULT_NAME");
		String kvUri = "https://" + keyVaultName + ".vault.azure.net";
		System.out.println("keyVault URI: " + kvUri);
		System.out.println("Secret Name: " + secretName);
		try {
			SecretClient secretClient = new SecretClientBuilder().vaultUrl(kvUri)
					.credential(new DefaultAzureCredentialBuilder().build()).buildClient();
			KeyVaultSecret retrievedSecret = secretClient.getSecret(secretName);

			// System.out.println(retrievedSecret);
			System.out.println(retrievedSecret.getValue());
			return retrievedSecret.getValue();

		} catch (Exception e) {
			System.out.println("Failed to retrieve secret " + secretName + "from Keyvault:" + keyVaultName);
			throw new Exception(e.getMessage());
		}
	}
}
