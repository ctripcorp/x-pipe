package com.ctrip.xpipe.utils;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;


/**
 * @author lishanglin
 * date 2024/6/20
 */
public class EncryptUtils {

    public static String encryptAES_ECB(String content, String secretKey) throws NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        if (StringUtil.isEmpty(content)) return content;

        byte[] key = makeKeyAES_ECB(secretKey);
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        byte[] encryptedBytes = cipher.doFinal(content.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(encryptedBytes);
    }

    public static String decryptAES_ECB(String encryptedContent, String secretKey) throws NoSuchPaddingException,
            NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        if (StringUtil.isEmpty(encryptedContent)) return encryptedContent;

        byte[] key = makeKeyAES_ECB(secretKey);
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, keySpec);
        byte[] decryptedBytes = cipher.doFinal(Base64.getDecoder().decode(encryptedContent));
        return new String(decryptedBytes, StandardCharsets.UTF_8);
    }

    private static byte[] makeKeyAES_ECB(String secretKey) {
        if (null != secretKey) {
            byte[] raw = secretKey.getBytes(StandardCharsets.UTF_8);
            if (raw.length >= 16) {
                return Arrays.copyOfRange(raw, 0, 16);
            } else {
                return extendWith(raw, 16, (byte) '\0');
            }
        } else {
            return extendWith(null, 16, (byte) '\0');
        }
    }

    private static byte[] extendWith(byte[] origin, int target, byte padding) {
        if (null == origin || origin.length == 0) {
            byte[] rst = new byte[target];
            Arrays.fill(rst, padding);
            return rst;
        } else if (origin.length < target) {
            byte[] rst = new byte[target];
            System.arraycopy(origin, 0, rst, 0, origin.length);
            Arrays.fill(rst, origin.length, rst.length, padding);
            return rst;
        } else {
            return origin;
        }
    }

}
