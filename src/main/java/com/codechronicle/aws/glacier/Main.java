package com.codechronicle.aws.glacier;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.*;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;


public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

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
