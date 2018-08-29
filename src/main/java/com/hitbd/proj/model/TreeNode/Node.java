package TreeNode;

import java.util.HashSet;
import java.util.Set;

public class Node {
public String name;
public String parent;
public  Set<String> childset = new HashSet<String>();

public Node(String name) {
  this.name=name;
}
public void setParent(String parent) {
 this.parent=parent; 
}
  public void addChild(String node) {
    childset.add(node);
  }
  @Override
  public boolean equals(Object obj) {
  if(obj instanceof Node) 
  return ((Node)obj).name.equals(this.name);
  return false;
  }
}
