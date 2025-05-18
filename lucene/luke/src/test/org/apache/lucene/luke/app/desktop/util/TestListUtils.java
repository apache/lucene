package org.apache.lucene.luke.app.desktop.util;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

import javax.swing.*;
import java.util.Arrays;
import java.util.List;

public class TestListUtils extends LuceneTestCase {

    @Test
    public void testGetAllItems() {
        JList<String> list = new JList<>(new String[]{"Item 1", "Item 2"});
        List<String> items = ListUtils.getAllItems(list);
        assertEquals(Arrays.asList("Item 1", "Item 2"), items);
        // test mutability
        items.add("Item 3");
        assertEquals(Arrays.asList("Item 1", "Item 2", "Item 3"), items);
    }
}
