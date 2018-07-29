package com.ctrip.xpipe.redis.proxy.echoserver;

import java.util.Random;

/**
 * @author chen.zhu
 * <p>
 * Jun 12, 2018
 */
public class LengthBasedGenerator implements MessageGenerator {

    private final int length;

    private char[] specialChars;

    private Random random = new Random();

    public LengthBasedGenerator(int length, char[] specialChars) {
        this.length = length;
        this.specialChars = specialChars;
    }

    @Override
    public String message() {
        return generate();
    }

    private String generate() {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < length; i++) {
            sb.append(newChar());
        }
        return sb.toString();
    }

    private char newChar() {
        char c;
        do {
            c = (char) (random.nextInt(25) + 'a');
        } while(inSpecialChars(c));
        return c;
    }

    private boolean inSpecialChars(char c) {
        for(char character : specialChars) {
            if(c == character) {
                return true;
            }
        }
        return false;
    }
}
