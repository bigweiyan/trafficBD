package com.hitbd.proj.action;

import com.hitbd.proj.Main;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestShell {
    @Before
    public void setup(){
        Main.loadSettings();
    }

    @Ignore
    @Test
    public void test(){
        new Shell().main();
    }
}
