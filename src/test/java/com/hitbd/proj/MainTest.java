package com.hitbd.proj;

import org.junit.Assert;
import org.junit.Test;

public class MainTest {
    @Test
    public void testLoadSettings() {
        Main.loadSettings();
        Assert.assertEquals(Settings.igniteHostAddress, "127.0.0.1");
    }
}
