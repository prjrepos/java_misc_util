package main.java.com.boots.voltage.encryption;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.file.FileAlreadyExistsException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.voltage.securedata.enterprise.AES;
import com.voltage.securedata.enterprise.LibraryContext;
import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.models.PathItem;
import main.java.com.boots.voltage.util.AzStorageUtilService;
import main.java.com.boots.voltage.util.VoltageUtilService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This Class does AES Encryption and Base64 Encoding using Voltage API for the
 * requested fields in the given input file
 * 
 * @author PG
 *
 */
public class EncryptionUtil {

	LibraryContext library = null;
	AES aes = null;
	String isHeaderStr = null;
	String delimiter = null;
	String encoding = null;
	List<Integer> listColumns = null;
	static final String NEW_LINE = "\n";
	String storageAccountName = null;
	String storageAccountKey = null;
	String rawDataDirectory = null;
	String storageContainerName = null;
	String encryptedDataDirectory = null;

	private static final Logger logger = LogManager.getLogger(EncryptionUtil.class);
	VoltageUtilService voltageService = new VoltageUtilService();
	AzStorageUtilService azStorageService = new AzStorageUtilService();

	public void encryptFile(String configFilePath) throws Exception {

		try {
			// Loading the vibesimplejava jar
			//logger.info("Java Lib Path: " + System.getProperty("java.library.path"));
			System.loadLibrary("vibesimplejava");

			logger.info("*******************************************************************");
			logger.info("Start Time : " + new java.util.Date());			
			logger.info("Configuration File Path : " + configFilePath);
			logger.info("*******************************************************************");

			setConfigurations(voltageService.createAESLibrary(configFilePath));
			// Read from sourceFilePath , Perform AES, Base64 Encode and Write output to sinkFilePath.
			readWriteFile();
			logger.info("*******************************************************************");
			logger.info("End Time :" + new java.util.Date());
			logger.info("*******************************************************************");
		} catch (Exception ex) {
			logger.info("Encryption Failed with Error: " + ex.getMessage());
			throw new Exception(ex.getMessage());
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
		if (configs[8] instanceof String)
			rawDataDirectory = (String) configs[8];
		if (configs[9] instanceof String)
			storageContainerName = (String) configs[9];
		if (configs[10] instanceof String)
			encryptedDataDirectory = (String) configs[10];

	}

	/**
	 * Method to read input file, encrypt the requested fields and write the
	 * file back to destination.
	 * 
	 * @param sourceFile
	 * @param sinkFile
	 * @return
	 * @throws Exception
	 */
	public boolean readWriteFile() throws Exception {
		BufferedReader objReader = null;
		String strCurrentLine;
		String path = null;
		String fileName = null;
		String sinkFileName = null;
		String dataToEncrypt = null;
		boolean isHeader = false;
		DataLakeServiceClient client = null;
		DataLakeFileSystemClient fsclient = null;
		DataLakeFileClient fileClient = null;
		InputStream encryptedData = null;
		StringBuffer sb = new StringBuffer("");

		logger.info("! inside readWriteFile !");

		try {

			client = AzStorageUtilService.GetDataLakeServiceClientByAccountKey(storageAccountName,
					storageAccountKey);
			fsclient = client.getFileSystemClient(storageContainerName);
			DataLakeDirectoryClient directoryClient = fsclient.getDirectoryClient(rawDataDirectory);
			PagedIterable<PathItem> blobs = directoryClient.listPaths();
			for (PathItem blobItem : blobs) {
				path = blobItem.getName();
				fileName = path.substring(path.lastIndexOf("/") + 1);
				fileClient = directoryClient.getFileClient(fileName);
				objReader = new BufferedReader(
						new InputStreamReader(fileClient.openInputStream().getInputStream(), encoding));
				sinkFileName = fileName.substring(0, fileName.lastIndexOf("."))
						+ "_"
						+ System.currentTimeMillis()
						+ "."
						+ fileName.substring(fileName.lastIndexOf(".") + 1);
				isHeader = new Boolean(isHeaderStr);
				// Check header is enabled or not, if enabled it skip the first line
				if (isHeader) {
					String header = objReader.readLine();
					sb.append(header + NEW_LINE);
				}
				while ((strCurrentLine = objReader.readLine()) != null) {
					String split_delimiter = "\\" + delimiter;
					String fileds[] = strCurrentLine.split(split_delimiter, -1);
					try {
						for (int j = 0; j < listColumns.size(); j++) {
							Integer fileColumIndex = listColumns.get(j);
							int arrayIndex = fileColumIndex - 1;
							dataToEncrypt = fileds[arrayIndex];
							dataToEncrypt = encryptData(dataToEncrypt);
							fileds[arrayIndex] = dataToEncrypt;
						}
						Integer counter = 1;
						for (String t : fileds) {
							// logger.info(counter+":"+fileds.length);
							if (fileds.length != counter)
								sb.append(t).append(delimiter);
							else
								sb.append(t);
							counter = counter + 1;

						}
						sb.append(NEW_LINE);
					} catch (IndexOutOfBoundsException iobe) {
						logger.info(
								"!!! Error occured in [readWriteFile] for following data, Skipping the record: "
										+ strCurrentLine);
						objReader.close();
						throw new Exception(
								"!!! Error occured in [readWriteFile] for following data, Skipping the record: "
										+ strCurrentLine);

					}

				}
				String joinedString = sb.toString();
				encryptedData = new ByteArrayInputStream(joinedString.getBytes(encoding));
				DataLakeDirectoryClient OutputDirectoryClient = fsclient.getDirectoryClient(encryptedDataDirectory);
				fileClient = OutputDirectoryClient.getFileClient(sinkFileName);
				fileClient.upload(encryptedData, joinedString.length(), false);
				objReader.close();
				sb.delete(0, sb.length());
			}
		} catch (FileAlreadyExistsException faee) {
			logger.info("Failed [ readWriteFile ] : " + faee.getMessage());
			throw new Exception("Failed [ readWriteFile ] : " + faee.getMessage());
		} catch (IOException ioe) {
			logger.info("Failed [ readWriteFile ] : " + ioe.getMessage());
			throw new Exception("Failed [ readWriteFile ] : " + ioe.getMessage());
		} catch (Exception e) {
			logger.info("Failed [ readWriteFile ] : " + e.getMessage());
			throw new Exception("Failed [ readWriteFile ] : " + e.getMessage());
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
	 * Method to perform AES encryption and Base64 Encode using Voltage API.
	 * 
	 * 
	 * @param plaintext
	 * @return Base64 output value of the encrypted text
	 * @throws Exception
	 */
	public String encryptData(String plaintext) throws Exception {
		String encodedStr = null;
		boolean wrapLines = false;

		try {
			// AES Encryption
			byte[] ciphertextBytes = aes.protect(plaintext.getBytes());
			// Convert the byte to Base64 Encode
			encodedStr = library.base64Encode(ciphertextBytes, wrapLines);
		} catch (Exception ex) {
			logger.info("Failed [encryptData]: " + ex.getMessage());
			throw new Exception("Failed [encryptData]: " + ex.getMessage());
		} catch (Throwable ex) {
			logger.info("Failed [encryptData]: Unexpected exception: " + ex);
			throw new Exception("Failed [encryptData]: " + ex.getMessage());
		}
		return encodedStr;

	}
}