package com.hitbd.proj;

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
            System.out.println("ImportAlarm FileName/Folder HBaseConfFile   #import alarm file to hbase");
            System.out.println("ImportDevice FileName/Folder import   #alarm file to ignite");
            System.out.println("CreateIgniteTable [hostname]  #create user_b, user_c and device table");
            return;
        }
        loadSettings();
        switch (args[0]) {
            case "ImportAlarm":
                HbaseUpload.main(args);
                break;
            case "ImportDevice":
                DeviceUpload.main(args);
                break;
            case "CreateIgniteTable":
                CreateIgniteTable.main(args);
                break;
            default:
                System.out.println("Usage: trafficBD Action [Parameter]");
                System.out.println("Actions:");
                System.out.println("ImportAlarm FileName/Folder HBaseConfFile   #import alarm file to hbase");
                System.out.println("ImportDevice FileName/Folder import   #alarm file to ignite");
                System.out.println("CreateIgniteTable [hostname]  #create user_b, user_c and device table");
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
