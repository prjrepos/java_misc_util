package main.java.com.boots.voltage.decryption;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.PathItem;
import com.voltage.securedata.enterprise.AES;
import com.voltage.securedata.enterprise.LibraryContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import main.java.com.boots.voltage.util.AzStorageUtilService;
import main.java.com.boots.voltage.util.VoltageUtilService;

/**
 * This Class does AES Decryption using Voltage Simple API for the requested
 * fields in the given input file.
 * 
 * @author PG
 *
 */
public class DecryptionUtil {

	LibraryContext library = null;
	AES aes = null;
	String isHeaderStr = null;
	String delimiter = null;
	String encoding = null;
	List<Integer> listColumns = null;
	static final String NEW_LINE = "\n";
	String storageAccountName = null;
	String storageAccountKey = null;
	String storageContainerName = null;
	String encryptedDataDirectory = null;
	String decryptedDataDirectory = null;
	private static final Logger logger = LogManager.getLogger(DecryptionUtil.class);
	VoltageUtilService voltageService = new VoltageUtilService();
	AzStorageUtilService azStorageService = new AzStorageUtilService();

	/**
	 * Main method to invoke the methods. It accepts three arguments.
	 * 
	 * 
	 * @param args
	 */

	public void decryptFile(String configFilePath)
			throws Exception {
		
		try {			

			logger.info("*******************************************************************");
			logger.info("Start Time : " + new java.util.Date());			
			logger.info("Configuration File Path : " + configFilePath);
			logger.info("*******************************************************************");

			// Reading the Config XML file and Creating the AES and Library object.
			setConfigurations(voltageService.createAESLibrary(configFilePath));
			// Read from sourceFilePath , Perform Base64Decode, AES Decryption and Write
			// output to sinkFilePath.
			readWriteFile();			
			logger.info("*******************************************************************");
			logger.info("End Time :" + new java.util.Date());
			logger.info("*******************************************************************");
		} catch (Exception ex) {
			logger.info("Decryption Failed with Error: " + ex.getMessage());
		}
	}

	public void setConfigurations(Object[] configs) {
		if (configs[0] instanceof String)
			encoding = (String) configs[0];
		if (configs[1] instanceof String)
			delimiter = (String) configs[1];
		if (configs[2] instanceof String)
			isHeaderStr = (String) configs[2];
		if (configs[3] instanceof AES)
			aes = (AES) configs[3];
		if (configs[4] instanceof LibraryContext)
			library = (LibraryContext) configs[4];
		listColumns = (List<Integer>) configs[5];
		if (configs[6] instanceof String)
			storageAccountName = (String) configs[6];
		if (configs[7] instanceof String)
			storageAccountKey = (String) configs[7];
		if (configs[9] instanceof String)
			storageContainerName = (String) configs[9];
		if (configs[10] instanceof String)
			encryptedDataDirectory = (String) configs[10];
		if (configs[11] instanceof String)
			decryptedDataDirectory = (String) configs[11];
	}

	/**
	 * Method to read input file, decrypt the requested fields and write the file
	 * back to destination.
	 * 
	 * @param sourceFile
	 * @param sinkFile
	 * @return
	 * @throws Exception
	 */
	public boolean readWriteFile() {

		BufferedReader objReader = null;
		String strCurrentLine;
		String dataToDecrypt = null;
		boolean isHeader = true;
		DataLakeServiceClient client = null;
		DataLakeFileSystemClient fsclient = null;
		DataLakeFileClient fileClient = null;
		String path = null;
		String fileName = null;
		String sinkFileName = null;
		StringBuffer sb = new StringBuffer("");
		InputStream decryptedData = null;

		try {

			client = AzStorageUtilService.GetDataLakeServiceClientByAccountKey(storageAccountName,
					storageAccountKey);
			fsclient = client.getFileSystemClient(storageContainerName);
			DataLakeDirectoryClient directoryClient = fsclient.getDirectoryClient(encryptedDataDirectory);
			PagedIterable<PathItem> blobs = directoryClient.listPaths();
			for (PathItem blobItem : blobs) {
				path = blobItem.getName();
				fileName = path.substring(path.lastIndexOf("/") + 1);
				if (fileName.endsWith(".processed")) {
					continue;
				}
				fileClient = directoryClient.getFileClient(fileName);
				objReader = new BufferedReader(new InputStreamReader(fileClient.openInputStream().getInputStream(),
						StandardCharsets.ISO_8859_1));
				sinkFileName = fileName.substring(0, fileName.lastIndexOf("."))
						+ "_"
						+ System.currentTimeMillis()
						+ "."
						+ fileName.substring(fileName.lastIndexOf(".") + 1);
				// Check header is enabled or not, if enabled it skip the first line;
				if (isHeader) {
					String header = objReader.readLine();
					sb.append(header + NEW_LINE);
				}
				while ((strCurrentLine = objReader.readLine()) != null) {
					String fields[] = strCurrentLine.split(delimiter, -1);
					for (int j = 0; j < listColumns.size(); j++) {
						Integer fileColumIndex = listColumns.get(j);
						int arrayIndex = fileColumIndex - 1;
						dataToDecrypt = fields[arrayIndex];
						dataToDecrypt = decryptData(dataToDecrypt);
						fields[arrayIndex] = dataToDecrypt;
					}
					String joinedString = StringUtils.join(fields, delimiter);
					sb.append(joinedString + NEW_LINE);
				}
				String joinedString = sb.toString();
				decryptedData = new ByteArrayInputStream(joinedString.getBytes(encoding));
				fileClient.rename(null, encryptedDataDirectory + "/" + fileName + ".processed");
				DataLakeDirectoryClient OutputDirectoryClient = fsclient.getDirectoryClient(decryptedDataDirectory);
				fileClient = OutputDirectoryClient.getFileClient(sinkFileName);
				fileClient.upload(decryptedData, joinedString.length(), false);
				objReader.close();
				sb.delete(0, sb.length());
			}

		} catch (UncheckedIOException uioe) {
			logger.info(
					"Failed [ readWriteFile ] - Occured when connecting with azure storage : " + uioe.getMessage());
			uioe.printStackTrace();
		} catch (IndexOutOfBoundsException iobe) {
			logger.info(
					"Failed [ readWriteFile ] - Occured when connecting with azure storage : " + iobe.getMessage());
			iobe.printStackTrace();
		} catch (IOException ioe) {
			logger.info("Failed [ readWriteFile ] : " + ioe.getMessage());
			ioe.printStackTrace();
		} finally {
			// Explicit delete, required for JNI
			if (aes != null) {
				aes.delete();
			}
			// Explicit delete, required for JNI
			if (library != null) {
				library.delete();
			}
		}
		return true;
	}

	/**
	 * Method to perform AES decryption and Base64 Decode using Voltage API.
	 * 
	 * @param encryptText
	 * @return decryptStr
	 * @throws Exception
	 */
	public String decryptData(String encryptText) {

		String decryptStr = null;
		boolean ignoreInvalidChars = true;
		try {
			// Decode the Base64 to perform the AES Decryption
			byte[] decodedResult = library.base64Decode(encryptText, ignoreInvalidChars);
			// AES Decryption
			byte[] access = aes.access(decodedResult);
			decryptStr = new String(access);
		} catch (Exception ex) {
			logger.info("Failed [decryptData]: " + ex.getMessage());
			ex.printStackTrace();
		}
		return decryptStr;

	}

}
