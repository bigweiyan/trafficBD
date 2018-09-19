package com.hitbd.proj;

import com.hitbd.proj.action.GenerateImeiCase;
import com.hitbd.proj.action.TestAlarmC;
import com.hitbd.proj.logic.hbase.HbaseUpload;
import com.hitbd.proj.logic.ignite.CreateIgniteTable;
import com.hitbd.proj.logic.ignite.DeviceUpload;
import org.apache.ignite.Ignition;

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
                TestAlarmC.main(args);
                IgniteSearch.getInstance().stop();
                break;
            case "GenerateImeiCase":
                GenerateImeiCase.main(args);
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
        try {
            settings = new File ("conf/settings");
            if (settings.exists()){
                Scanner scanner = new Scanner(settings);
                while (scanner.hasNext()) {
                    String[] conf = scanner.nextLine().split("=");
                    String key = conf[0].trim();
                    String value = conf[1].trim();
                    switch (key) {
                        case "igniteHostAddress":
                            Settings.igniteHostAddress = value;
                            break;
                        case "logDir":
                            Settings.logDir = value;
                            break;
                        default:
                            System.out.println("Cannot resolve attribute: " + key);
                    }
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }catch (IndexOutOfBoundsException e2) {
            System.out.println("config file should have format like key:value");
            e2.printStackTrace();
        }
    }
}
