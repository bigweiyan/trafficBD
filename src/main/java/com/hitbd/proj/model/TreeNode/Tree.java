package TreeNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.springframework.util.CollectionUtils;

public class Tree extends Object{
public  ArrayList<Node> Tre=new ArrayList<Node>();

public  int indexOf(String node) {
  for(int i=0;i<Tre.size();i++) {
    if(Tre.get(i).name.equals(node)) {
      return i;
    }
  }
  return -1;
}

public  void update(String str) {
  String []tem=str.replaceAll(" ","").split(",");
  for(int i=0;i<tem.length;i++) {
    if(!Tre.contains(new Node(tem[i]))) {
      Node node=new Node(tem[i]);
      if(i!=0) {
        String father=tem[i-1];
        node.setParent(father);
      }
      String thechild=str.substring(str.indexOf(tem[i])+tem[i].length()+1, str.length());
      String [] children=thechild.split(",");    
      Set<String> childrenset = new HashSet<>(Arrays.asList(children)); 
      node.childset=childrenset;     
      Tre.add(node);
    }
    else {
      int j=indexOf(tem[i]);
      if(i!=0) {
        Tre.get(j).setParent(tem[i-1]);
      }
      String thechild=str.substring(str.indexOf(tem[i])+tem[i].length()+1, str.length());
      String [] children=thechild.split(",");    
      Set<String> childrenset = new HashSet<>(Arrays.asList(children)); 
      Tre.get(j).childset.addAll(childrenset);
    }
  }
}
}
