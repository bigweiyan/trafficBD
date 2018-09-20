package com.hitbd.proj;

import org.junit.Assert;
import org.junit.Test;

public class MainTest {
    @Test
    public void testLoadSettings() {
        Main.loadSettings();
        Assert.assertEquals(1, Settings.Test.QUERY_THREAD_PER_TEST);
    }
}
