package com.hitbd.proj;

import com.hitbd.proj.action.GenerateImeiCase;
import com.hitbd.proj.action.TestAlarmC;
import com.hitbd.proj.action.TestImeiSearch;
import com.hitbd.proj.logic.hbase.HbaseUpload;
import com.hitbd.proj.logic.ignite.CreateIgniteTable;
import com.hitbd.proj.logic.ignite.DeviceUpload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: trafficBD Action [Parameter]");
            System.out.println("Actions:");
            System.out.println("ImportAlarm FileName/Folder HBaseConfFile [copies]  #import alarm file to hbase");
            System.out.println("ImportDevice FileName/Folder [copies] #import device file to ignite");
            System.out.println("CreateIgniteTable [hostname]  #create user_b, user_c and device table");
            System.out.println("GenerateImeiCase FileName/Folder  #generate imei test case");
            return;
        }
        loadSettings();
        switch (args[0]) {
            case "ImportAlarm":
                HbaseUpload.main(args);
                IgniteSearch.getInstance().stop();
                break;
            case "ImportDevice":
                DeviceUpload.main(args);
                IgniteSearch.getInstance().stop();
                break;
            case "CreateIgniteTable":
                CreateIgniteTable.main(args);
                IgniteSearch.getInstance().stop();
                break;
            case "AlarmC":
                new TestAlarmC().main(args);
                IgniteSearch.getInstance().stop();
                break;
            case "GenerateImeiCase":
                new GenerateImeiCase().main(args);
                break;
            case "TestImeiSearch":
                new TestImeiSearch().main(args);
                break;
            default:
                System.out.println("Usage: trafficBD Action [Parameter]");
                System.out.println("Actions:");
                System.out.println("ImportAlarm FileName/Folder HBaseConfFile [copies]  #import alarm file to hbase");
                System.out.println("ImportDevice FileName/Folder [copies] #import device file to ignite");
                System.out.println("CreateIgniteTable [hostname]  #create user_b, user_c and device table");
                System.out.println("GenerateImeiCase FileName/Folder  #generate imei test case");
        }

    }

    static void loadSettings() {
        File settings;
        File hbaseSite;
        try {
            settings = new File ("conf/settings");
            if (settings.exists()){
                Scanner scanner = new Scanner(settings);
                while (scanner.hasNext()) {
                    String[] conf = scanner.nextLine().split("=");
                    String key = conf[0].trim();
                    String value = conf[1].trim();
                    try {
                        switch (key) {
                            case "logDir":
                                Settings.LOG_DIR = value;
                                break;
                            case "test.imei_per_query":
                                Settings.Test.IMEI_PER_QUERY = Integer.valueOf(value);
                                break;
                            case "test.query_thread_per_test":
                                Settings.Test.QUERY_THREAD_PER_TEST = Integer.valueOf(value);
                                break;
                            case "test.wait_until_finish":
                                Settings.Test.WAIT_UNTIL_FINISH = value.equals("true");
                                break;
                            default:
                                System.out.println("Cannot resolve attribute: " + key);
                        }
                    }catch (RuntimeException e){
                        System.out.println("Cannot resolve attribute: " + key);
                    }
                }
            }

            // create hbase connection
            hbaseSite = new File("conf/hbase-site.xml");
            if (hbaseSite.exists()) {
                Configuration configuration = HBaseConfiguration.create();
                configuration.addResource(hbaseSite.getAbsolutePath());
                Settings.HBASE_CONFIG = configuration;
            }
            if (Settings.HBASE_CONFIG == null) {
                Configuration configuration = HBaseConfiguration.create();
                configuration.addResource("/usr/hbase-1.3.2.1/conf/hbase-site.xml");
                Settings.HBASE_CONFIG = configuration;
            }
        }catch (IOException e){
            e.printStackTrace();
        }catch (IndexOutOfBoundsException e2) {
            System.out.println("config file should have format like key:value");
            e2.printStackTrace();
        }
    }
}
