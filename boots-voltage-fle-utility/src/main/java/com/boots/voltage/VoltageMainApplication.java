package main.java.com.boots.voltage;

import main.java.com.boots.voltage.encryption.EncryptionUtil;
import main.java.com.boots.voltage.decryption.DecryptionUtil;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VoltageMainApplication {

    private static final Logger logger = LogManager.getLogger(VoltageMainApplication.class);
    private static String[] metadataPath = { ".", "trustStore", "trustStore/inc", "cache" };

    public static void main(String[] args) {

        EncryptionUtil encryptObj = new EncryptionUtil();
        DecryptionUtil decryptObj = new DecryptionUtil();

        try {
            //VoltageMainApplication main = new VoltageMainApplication();
            //main.printLocalMetadataFiles();

            // Loading the vibesimplejava jar
            String OS = System.getProperty("os.name");
            logger.info("Operating System: " + OS);
            if (OS.startsWith("Windows"))
                System.loadLibrary("vibesimplejava");
            else                
                System.load("/mnt/batch/tasks/startup/wd/libvibesimplejava.so");

            encryptObj.encryptFile(args[0]);
            decryptObj.decryptFile(args[0]);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void printLocalMetadataFiles() {
        for (String path : metadataPath) {
            File folder = new File(path);
            if (folder.exists()) {
                for (final File fileEntry : folder.listFiles()) {
                    if (fileEntry.isDirectory()) {
                        continue;
                    } else {
                        try {
                            logger.info("File found in " + fileEntry.getCanonicalPath());
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }
            } else {
                logger.info(path + " Metadata folder Not found ");
            }
        }
    }

}
