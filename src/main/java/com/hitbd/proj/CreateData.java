package com.hitbd.proj;

import java.io.*;

public class CreateData {
    public static void main(String[] args) {
        getTestData3();
    }
    public  static void getTestData3(){
        String temp = "g://data//imeiCase";
        String treefile = "g://data//test//4.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(treefile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(temp))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        int count = 0;
        try {
            while((linetxt = buffer.readLine())!=null) {
                count++;
                bw.write(linetxt);
                bw.newLine();
                bw.flush();
                if(count==100)
                    break;;
            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public static void testCountImei(){
        String inputfile = "g://data//test//2.csv";
        String outputfile = "g://data//result//test2.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(outputfile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputfile))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        int count = 0;
        try {
            while((linetxt = buffer.readLine())!=null) {
                if((linetxt = buffer.readLine())!=null) {
                    String imei = linetxt;
                    if((linetxt = buffer.readLine())!=null) {
                        String userid = linetxt;
                        if((linetxt = buffer.readLine())!=null) {
                            count++;
                        }
                    }
                }
            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public  static void getTestData2(){
        String temp = "g://data//alarmall.csv";
        String treefile = "g://data//test//3.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(treefile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(temp))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        try {
            while((linetxt = buffer.readLine())!=null) {
                String parentid = new String();
                String imei = new String();
                String userid = new String();
                String temp1 = linetxt, temp2 = new String(), temp3 = new String();
                if ((linetxt = buffer.readLine()) != null)
                    temp2 = linetxt;
                if ((linetxt = buffer.readLine()) != null)
                    temp3 = linetxt;
                String[] items = temp1.split(",");
                imei = items[2] + ",";
                userid = items[1] + ",";
                parentid = items[0]+",";
                items = temp2.split(",");
                imei = imei + items[2] + ",";
                userid = userid + items[1] + ",";
                parentid = parentid + items[0]+",";
                items = temp3.split(",");
                imei = imei + items[2];
                userid = userid + items[1];
                parentid = parentid +items[0];

                bw.write(parentid);
                bw.newLine();
                bw.write(imei);
                bw.newLine();
                bw.write(userid);
                bw.newLine();
                bw.flush();
            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public static  void getTestData(){
        String temp = "g://data//test//1.csv";
        String treefile = "g://data//test//2.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(treefile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(temp))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        try {
            while((linetxt = buffer.readLine())!=null) {
                String imei = new String();
                String userid = new String();
                String temp1 = linetxt,temp2 = new String(),temp3 = new String();
                if((linetxt = buffer.readLine())!=null)
                    temp2 = linetxt;
                if((linetxt = buffer.readLine())!=null)
                    temp3 = linetxt;
                String[] items = temp1.split(",");
                imei = items[2]+",";
                userid = items[1]+",";
                items = temp2.split(",");
                imei = imei+items[2]+",";
                userid = userid+items[1]+",";
                items = temp3.split(",");
                imei = imei+items[2];
                userid = userid+items[1];

                bw.write(imei);
                bw.newLine();
                bw.write(userid);
                bw.newLine();
                bw.flush();

            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("成功计算得到所有告警的摘要");
    }
    public static void getAlarmCountByStatus(){
        String temp = "g://data//alarmall.csv";
        String treefile = "g://data//test//1.csv";
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(treefile), true));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader buffer = null;
        try {
            buffer = new BufferedReader(new InputStreamReader(new FileInputStream(new File(temp))));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        String linetxt ;
        try {
            while((linetxt = buffer.readLine())!=null) {
                String[] items = linetxt.split(",");
                for(int i =3;i<items.length;i=i+2){
                    if(items[i].equals("overSpeed")||items[i].equals("6")){
                        bw.write(linetxt);
                        bw.newLine();
                        bw.flush();
                    }
                }

            }
            if (bw != null) bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("成功计算得到所有告警的摘要");
    }

}
