package main.java.com.boots.voltage;

import main.java.com.boots.voltage.encryption.EncryptionUtil;



import main.java.com.boots.voltage.decryption.DecryptionUtil;

public class VoltageMainApplication {

    public static void main(String[] args) {

        EncryptionUtil encryptObj = new EncryptionUtil();
        DecryptionUtil decryptObj = new DecryptionUtil();

               
        try {
            //encryptObj.encryptFile("boots-voltage-fle-utility/src/resources/config/customer-config.xml");            
            //decryptObj.decryptFile("boots-voltage-fle-utility/src/resources/config/customer-config.xml");
            encryptObj.encryptFile(args[0]);
            decryptObj.decryptFile(args[0]);            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
}
