package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 19, 2018
 */
public class CloseStateTest extends AbstractTest {


    @Test
    public void testClosing() {

        CloseState closeState = new CloseState();

        Assert.assertTrue(closeState.isOpen());

        closeState.setClosing();
        Assert.assertTrue(closeState.isClosing());

        closeState.setClosed();
        Assert.assertTrue(closeState.isClosed());

        try {
            closeState.setClosing();
            Assert.fail();
        } catch (Exception e) {

        }

    }

    @Test
    public void testMakeSureOpen() {

        CloseState closeState = new CloseState();
        closeState.makeSureOpen();
        closeState.setClosing();

        try {
            closeState.makeSureOpen();
            Assert.fail();
        } catch (Exception e) {
            //ignore
        }

        closeState.setClosed();
        try {
            closeState.makeSureOpen();
            Assert.fail();
        } catch (Exception e) {
            //ignore
        }
    }

    @Test
    public void testMakeSureClosed() {

        CloseState closeState = new CloseState();
        closeState.makeSureNotClosed();

        closeState.setClosing();
        closeState.makeSureNotClosed();

        closeState.setClosed();
        try {
            closeState.makeSureOpen();
            Assert.fail();
        } catch (Exception e) {
            //ignore
        }

    }


}
