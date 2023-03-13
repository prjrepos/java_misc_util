package main.java.com.boots.voltage.util;

import com.voltage.securedata.enterprise.AES;
import com.voltage.securedata.enterprise.FPE;
import com.voltage.securedata.enterprise.LibraryContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.ArrayList;
import java.util.List;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VoltageUtilService {

    LibraryContext library = null;
    AES aes = null;
    FPE fpe = null;
    String policyURL = null;
    String identity = null;
    String serviceAccount = null;
    String trustStorePath = null;
    String cachePath = null;
    String isHeaderStr = null;
    String delimiter = null;
    String sharedSecret = null;
    String authMethod = null;
    String userCredential = null;
    String encoding = null;
    String storageAccountName = null;
    String storageAccountKey = null;
    List<Integer> listColumns = null;
    String rawDataDirectory = null;
    String storageContainerName = null;
    String encryptedDataDirectory = null;
    String decryptedDataDirectory = null;   

    private static final Logger logger = LogManager.getLogger(VoltageUtilService.class);

    /**
     * Method to create library and AES object for encryption operation.
     * 
     * @param userCredential
     * @throws Exception
     * 
     */
    public Object[] createAESLibrary(String configFilePath) throws Exception {
        try {
            // Reading the Config XML file.
            readConfigXML(configFilePath);
            // Create the library context for crypto operations
            library = new LibraryContext.Builder()
                    .setPolicyURL(policyURL)
                    .setFileCachePath(cachePath)
                    .setTrustStorePath(trustStorePath)                  
                    .setClientIdProduct("Simple_API_Java_FPE_Sample", "1.2").build();
            /*
             * Creating the AES object Username and Password authentication
             * method.
             */
            if (authMethod.toLowerCase().equals("sharedsecret")) {
                aes = library.getAESBuilder().setIdentity(identity).setSharedSecret(sharedSecret).build();
            } else if (authMethod.toLowerCase().equals("usernamepassword")) {
                aes = library.getAESBuilder().setIdentity(identity).setUsernamePassword(serviceAccount, userCredential)
                        .build();
            } else {
                throw new Exception("Invalid authentication method from config:" + authMethod);
            }
            Object[] returnableConfigs = { 
                    encoding,
                    delimiter,
                    isHeaderStr,
                    aes, 
                    library,
                    listColumns,
                    storageAccountName,
                    storageAccountKey,
                    rawDataDirectory,
                    storageContainerName,
                    encryptedDataDirectory,
                    decryptedDataDirectory};                   
                    
            return returnableConfigs;

        } catch (Exception ex) {
            // ex.printStackTrace();
            logger.info("Failed [createLibrary] AES: " + ex.getMessage());
            throw new Exception("Failed [createLibrary] AES: " + ex.getMessage());
        } catch (Throwable ex) {
            // ex.printStackTrace();
            logger.info("Failed [createLibrary] AES: Unexpected exception: " + ex);
            throw new Exception("Failed [createLibrary] AES: " + ex.getMessage());
        }

    }

    /**
     * Method to create library and AES object for encryption operation.
     * 
     * @param userCredential
     * @throws Exception
     * 
     */
    public Object[] createFPELibrary(String configFilePath) throws Exception {
        try {
            // Reading the Config XML file.
            readConfigXML(configFilePath);
            // Create the library context for crypto operations
            library = new LibraryContext.Builder()
                    .setPolicyURL(policyURL)
                    .setFileCachePath(cachePath)
                    .setTrustStorePath(trustStorePath)                   
                    .setClientIdProduct("Simple_API_Java_FPE_Sample", "1.2").build();
            /*
             * Creating the AES object Username and Password authentication
             * method.
             */
            if (authMethod.toLowerCase().equals("sharedsecret")) {
                //aes = library.getAESBuilder().setIdentity(identity).setSharedSecret(sharedSecret).build();
                fpe = library.getFPEBuilder().setFormat("AUTO").setIdentity(identity).setSharedSecret(sharedSecret).build();
            } else if (authMethod.toLowerCase().equals("usernamepassword")) {
                //aes = library.getAESBuilder().setIdentity(identity).setUsernamePassword(serviceAccount, userCredential).build();
                fpe = library.getFPEBuilder().setIdentity(identity).setUsernamePassword(serviceAccount, userCredential).build();
            } else {
                throw new Exception("Invalid authentication method from config:" + authMethod);
            }
            Object[] returnableConfigs = { 
                    encoding,
                    delimiter,
                    isHeaderStr,
                    //aes,
                    fpe,
                    library,
                    listColumns,
                    storageAccountName,
                    storageAccountKey,
                    rawDataDirectory,
                    storageContainerName,
                    encryptedDataDirectory,
                    decryptedDataDirectory};                   
                    
            return returnableConfigs;

        } catch (Exception ex) {
            // ex.printStackTrace();
            logger.info("Failed [createLibrary] AES: " + ex.getMessage());
            throw new Exception("Failed [createLibrary] AES: " + ex.getMessage());
        } catch (Throwable ex) {
            // ex.printStackTrace();
            logger.info("Failed [createLibrary] AES: Unexpected exception: " + ex);
            throw new Exception("Failed [createLibrary] AES: " + ex.getMessage());
        }

    }

    /**
     * Method to parse configuration xml file
     * 
     * We can keep in a separate utility class to read this XML by returning the
     * result as POJO
     * 
     * @param xmlFilePath
     * @throws Exception
     */
    public void readConfigXML(String xmlFilePath) throws Exception {

        try {

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            listColumns = new ArrayList<Integer>();
            List<String> listDescriptionColumns = new ArrayList<String>();
            File fXmlFile = new File(xmlFilePath);
            Document doc = dBuilder.parse(fXmlFile);
            doc.getDocumentElement().normalize();
            NodeList nList = doc.getElementsByTagName("Field");
            NodeList confList = doc.getElementsByTagName("Config");
            Node cNode = confList.item(0);
            Element cElem = (Element) cNode;
            policyURL = cElem.getElementsByTagName("Policy-URL").item(0).getTextContent();
            identity = cElem.getElementsByTagName("Identity").item(0).getTextContent();
            trustStorePath = cElem.getElementsByTagName("TrustStorePath").item(0).getTextContent();
            cachePath = cElem.getElementsByTagName("CachePath").item(0).getTextContent();
            isHeaderStr = cElem.getElementsByTagName("Header").item(0).getTextContent();
            delimiter = cElem.getElementsByTagName("Delimiter").item(0).getTextContent();
            authMethod = cElem.getElementsByTagName("AuthMethod").item(0).getTextContent();
            serviceAccount = cElem.getElementsByTagName("ServiceAccount").item(0).getTextContent();
            encoding = cElem.getElementsByTagName("Encoding").getLength() == 0 ? StandardCharsets.UTF_8.displayName()
                    : cElem.getElementsByTagName("Encoding").item(0).getTextContent();
            storageAccountName = cElem.getElementsByTagName("StorageAccountName").item(0).getTextContent();
            storageAccountKey = cElem.getElementsByTagName("StorageAccountKey").item(0).getTextContent();
            storageContainerName = cElem.getElementsByTagName("StorageContainerName").item(0).getTextContent();
            rawDataDirectory = cElem.getElementsByTagName("RawDataDirectory").item(0).getTextContent();
            encryptedDataDirectory = cElem.getElementsByTagName("EncryptedDataDirectory").item(0).getTextContent();
            decryptedDataDirectory = cElem.getElementsByTagName("DecryptedDataDirectory").item(0).getTextContent();

            if (authMethod.toLowerCase().equals("sharedsecret")) {
                sharedSecret = cElem.getElementsByTagName("SharedSecret").item(0).getTextContent();
            } else if (authMethod.toLowerCase().equals("usernamepassword")) {
                userCredential = cElem.getElementsByTagName("UserCredential").item(0).getTextContent();
            } else {
                throw new Exception("Invalid authentication method from config:" + authMethod);
            }

            logger.info("************** Voltage & File Properties *************************");
            logger.info("Policy Name 	:" + policyURL);
            logger.info("Identity 		:" + identity);
            logger.info("TrustStorePath 	: " + trustStorePath);
            logger.info("CachePath 		:" + cachePath);
            logger.info("Header Enabled	: " + isHeaderStr);
            logger.info("ServiceAccount 	:" + serviceAccount);
            logger.info("Delimiter		: " + delimiter);
            logger.info("Encoding		: " + encoding);
            logger.info("*******************************************************************");
            for (int i = 0; i < nList.getLength(); i++) {
                Node nNode = nList.item(i);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element elem = (Element) nNode;

                    Node node1 = elem.getElementsByTagName("Column").item(0);
                    String colId = node1.getTextContent();
                    String colDesc = elem.getElementsByTagName("Description").item(0).getTextContent();

                    listColumns.add(Integer.parseInt(colId));
                    listDescriptionColumns.add(colDesc);

                }
            }

            logger.info("Fields going to encrypt/decrypt: " + listDescriptionColumns);

        } catch (IOException ioe) {
            logger.info("Failed [readConfigXML]: " + ioe.getMessage());
            throw new Exception("Error reading config file - " + ioe.getMessage());
        } catch (Exception e) {
            logger.info("Failed [readConfigXML]: " + e.getMessage());
            throw new Exception("Error reading config file - " + e.getMessage());
        }
    }

}
