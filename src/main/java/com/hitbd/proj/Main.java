package com.hitbd.proj;

import com.hitbd.proj.action.*;
import com.hitbd.proj.logic.hbase.HbaseUpload;
import com.hitbd.proj.logic.hbase.UserCUpload;
import com.hitbd.proj.logic.ignite.CreateIgniteTable;
import com.hitbd.proj.logic.ignite.DeviceUpload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: trafficBD Action [Parameter]");
            System.out.println("Actions:");
            System.out.println("ImportAlarm FileName/Folder HBaseConfFile [copies]  #import alarm file to hbase");
            System.out.println("ImportDevice FileName/Folder [copies] #import device file to ignite");
            System.out.println("ImportUserC FileName/Folder  #import device file to ignite");
            System.out.println("CreateIgniteTable [hostname]  #create user_b, user_c and device table");
            System.out.println("GenerateImeiCase FileName/Folder length  #generate imei test case");
            System.out.println("TestImeiSearch  #test query by imei");
            System.out.println("TestUserSearch  #test query by imei");
            System.out.println("shell");
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
            case "ImportUserC":
                UserCUpload.main(args);
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
            case "TestUserSearch":
                new TestUserSearch().main(args);
                IgniteSearch.getInstance().stop();
                break;
            case "TestHbaseSearch":
                new TestHbaseSearch().main(args);
                IgniteSearch.getInstance().stop();
                break;
            case "TestUpdate":
                new TestUpdate().main(args);
                break;
            case "TestOverSpeedSearch":
                new TestOverSpeedSearch().main(args);
                IgniteSearch.getInstance().stop();
                break;
            case "shell":
                new Shell().main();
                IgniteSearch.getInstance().stop();
                break;
            default:
                System.out.println("Usage: trafficBD Action [Parameter]");
                System.out.println("Actions:");
                System.out.println("ImportAlarm FileName/Folder HBaseConfFile [copies]  #import alarm file to hbase");
                System.out.println("ImportDevice FileName/Folder [copies] #import device file to ignite");
                System.out.println("ImportUserC FileName/Folder  #import device file to ignite");
                System.out.println("CreateIgniteTable [hostname]  #create user_b, user_c and device table");
                System.out.println("GenerateImeiCase FileName/Folder length  #generate imei test case");
                System.out.println("TestImeiSearch  #test query by imei");
                System.out.println("TestUserSearch  #test query by imei");
                System.out.println("shell");
        }

    }

    public static void loadSettings() {
        File settings;
        File hbaseSite;
        try {
            settings = new File ("conf/settings");
            if (settings.exists()){
                Scanner scanner = new Scanner(settings);
                while (scanner.hasNext()) {
                    String s = scanner.nextLine();
                    if (!s.contains("=")) continue;
                    String[] conf = s.split("=");
                    String key = conf[0].trim();
                    String value = conf[1].trim();
                    try {
                        switch (key) {
                            case "logDir":
                                Settings.LOG_DIR = value;
                                break;
                            case "max_worker_thread":
                                Settings.MAX_WORKER_THREAD = Integer.valueOf(value);
                                break;
                            case "max_devices_per_worker":
                                Settings.MAX_DEVICES_PER_WORKER = Integer.valueOf(value);
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
                            case "test.start_time_default":
                                Settings.Test.START_TIME_DEFAULT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(value).getTime();
                                break;
                            case "test.start_time_option":
                                Settings.Test.START_TIME_OPTION = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(value).getTime();
                                break;
                            case "test.start_time":
                                Settings.Test.START_TIME = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(value).getTime();
                                break;
                            case "test.end_time":
                                Settings.Test.END_TIME = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(value).getTime();
                                break;
                            case "test.show_top_result":
                                Settings.Test.SHOW_TOP_RESULT = value.equals("true");
                                break;
                            case "test.show_all_result":
                                Settings.Test.SHOW_ALL_RESULT = value.equals("true");
                                break;
                            case "test.result_size":
                                Settings.Test.RESULT_SIZE = Integer.parseInt(value);
                                break;
                            case "import_time_shift":
                                Settings.IMPORT_TIME_SHIFT = Long.parseLong(value);
                                break;
                            default:
                                System.out.println("Cannot resolve attribute: " + key);
                        }
                    }catch (ParseException | RuntimeException e){
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
