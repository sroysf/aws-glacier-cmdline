package com.codechronicle.aws.glacier;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;


public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);

    private static int ONE_MEGABYTE = 1024 * 1024;
    private static int PART_SIZE = ONE_MEGABYTE * 16;

    public static void main(String[] args) throws IOException, FilePartException {

    }

    public static void mainX(String[] args) {
        File f = new File("/home/saptarshi.roy/Downloads/ubuntu-10.04.4-server-amd64.iso");

        // First, decide on part size
        if (f.length() < PART_SIZE) {
            // Just upload it all at once
        } else {
            // Do a multi-part upload
            uploadMultiPart(f);
        }

        System.out.println(f.length());
    }

    private static void uploadMultiPart(File f) {
        int numParts = (int)(f.length() / PART_SIZE);
        System.out.println("Num parts = " + numParts);
        byte[] buffer = new byte[PART_SIZE];

        RandomAccessFile raf = null;
        FileOutputStream os = null;
        try {
            File outfile = new File("/tmp/testOutput.txt");
            os = new FileOutputStream(outfile);

            raf = new RandomAccessFile(f, "r");

            for (int i=0; i<numParts; i++) {
                System.out.println("Part " + i + " = " + i);
                raf.seek(i * PART_SIZE);
                raf.read(buffer);
                os.write(buffer);
            }

            int finalPartStart = (numParts * PART_SIZE);
            long finalPartSize = f.length() - finalPartStart;
            raf.seek(finalPartStart);
            raf.read(buffer, 0, (int)finalPartSize);
            os.write(buffer, 0, (int)finalPartSize);

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            if (raf != null) IOUtils.closeQuietly(raf);
            if (os != null) IOUtils.closeQuietly(os);
        }
    }

    private static void uploadPart(RandomAccessFile raf, int partNum, int partSize) {

        long filePointerLocation = partNum * partSize;

    }

    /**
	 * @param args
	 */
	public static void testListVaults(String[] args) throws Exception {

        Properties props = loadAWSCredentials();
        AWSCredentials credentials = new BasicAWSCredentials(props.getProperty("accessKey"), props.getProperty("secretKey"));

        AmazonGlacierClient agc = new AmazonGlacierClient(credentials);
        agc.setEndpoint(props.getProperty("endPoint"));

//        CreateVaultRequest cvRequest = new CreateVaultRequest(props.getProperty("accountId"), "PersonalMedia");
//        CreateVaultResult cvResult = agc.createVault(cvRequest);
//        System.out.println("Location = " + cvResult.getLocation());

        ListVaultsRequest lvRequest = new ListVaultsRequest(props.getProperty("accountId"));
        ListVaultsResult lvResult = agc.listVaults(lvRequest);
        List<DescribeVaultOutput> vaultDescriptions = lvResult.getVaultList();
        for (DescribeVaultOutput vaultDescription : vaultDescriptions) {
            System.out.println(vaultDescription.getVaultName() + " -- " + vaultDescription.getCreationDate());
        }
	}

    private static Properties loadAWSCredentials() {

        String userHome = System.getenv("HOME");
        File credFile = new File(userHome + "/.aws/aws.properties");

        Properties props = new Properties();
        FileReader fr = null;

        try {
            fr = new FileReader(credFile);
            props.load(fr);
            return props;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            IOUtils.closeQuietly(fr);
        }
    }

}
