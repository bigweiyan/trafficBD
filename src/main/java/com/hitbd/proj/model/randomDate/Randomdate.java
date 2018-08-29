package randomDate;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Randomdate {
//返回2007-01-01到2007-03-01的一个随机日期  
 private static  String a=null;;
  public static void main(String[] args) throws ParseException {
    String str="2214,2216";
    String[] temp = str.replaceAll("\"", "").split(",");
    int len=temp.length;
    StringBuffer date = new StringBuffer();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date initaldate=format.parse("2010-01-01 00:00:00");
    Date randomdate=Randomdate.randomDate("2019-05-14 00:00:00", "2039-05-14 00:00:00");
    long initial=(randomdate.getTime()-initaldate.getTime())/(1000*3600*24);
    date.append(temp[len-1] +","+ initial+",");
    if(len>3) {
      for(int i=len-2;i>len-4;i--){
        date.append(temp[i]+","+(initial+15*(len-1-i))+",");
        }
      for(int i=len-4;i>=0;i--) {
        date.append(temp[i]+","+0+",");
      }
    }
    else {
      for(int i=len-2;i>=0;i--){
      date.append(temp[i]+","+(initial+15*(len-1-i))+",");
      }
    }
    date.deleteCharAt(date.length() - 1);
    System.out.println(date);
  }  
  /** 
   * 获取随机日期 
   *  
   * @param beginDate 
   *            起始日期，格式为：yyyy-MM-dd 
   * @param endDate 
   *            结束日期，格式为：yyyy-MM-dd 
   * @return 
   */  

  public static Date randomDate(String beginDate, String endDate) {  
      try {  
          SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
          Date start = format.parse(beginDate);// 构造开始日期  
          Date end = format.parse(endDate);// 构造结束日期  
          if (start.getTime() >= end.getTime()) {  
              return null;  
          }  
          long date = random(start.getTime(), end.getTime());  
          return new Date(date);
      } catch (Exception e) {  
          e.printStackTrace();  
      }  
      return null;  
  }  

  private static long random(long begin, long end) {  
      long rtn = begin + (long) (Math.random() * (end - begin));  
      // 如果返回的是开始时间和结束时间，则递归调用本函数查找随机值  
      if (rtn == begin || rtn == end) {  
          return random(begin, end);  
      }  
      return rtn;  
  }  
}
