package org.apache.lucene.test;

import java.util.List;
import java.util.ArrayList;

/**
 * Test class with formatting violations
 */
public class TestJavaFormatting{
  private List<String>items=new ArrayList<>();

  public void addItem(String item){
    if(item!=null&&!item.isEmpty()){
      items.add(item);
    }else{
      throw new IllegalArgumentException("Item cannot be null or empty");
    }
  }

  public List<String> getItems(){return items;}
}
