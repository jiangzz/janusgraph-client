package com.jd;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

public class EncodeUtil {
    /**
     * 使用SHA256加密
     *
     * @param toEncode 要加密的字符串
     * @param salt     盐
     * @param roll     重复加密次数,不要超过11次
     * @return 加密后的字符串
     */
    public static String encodeBySha(String toEncode, String salt, Integer roll) {
        if (StringUtils.isEmpty(toEncode))
            throw new RuntimeException("加密字符串为空");
        if (roll < 0 || roll > 11)
            throw new RuntimeException("重复加密次数有误");

        //要加密的字符串和盐组合
        String encodeStr = toEncode + salt;
        byte[] bytes = encodeStr.getBytes();
        //重复一次等与加密两次
        for (int i = 0; i < roll + 1; i++) {
            //使用org.apache.commons.codec的工具类对字符串进行SHA256加密
            bytes = DigestUtils.sha256(bytes);
        }
        //将加密好的byte转换为16进制字符串
        encodeStr = Hex.encodeHexString(bytes);

        return encodeStr;
    }

    /**
     * 使用SHA256加密
     *
     * @param toEncode 要加密的字符串
     * @param salt     盐
     * @return
     */
    public static String encodeBySha(String toEncode, String salt) {
        return encodeBySha(toEncode, salt, 0);
    }

    /**
     * 使用SHA256加密
     *
     * @param toEncode 要加密的字符串
     * @return
     */
    public static String encodeBySha(String toEncode) {
        return encodeBySha(toEncode, null, 0);
    }

    /**
     * 生成盐值;其长度在17~29之间，使用A~Z,a~z,0~9组成随机字符串
     *
     * @return 盐
     */
    private static String generateSalt() {
        return RandomStringUtils.randomAlphanumeric(17, 29);
    }

    public static void main(String[] args) {
        String sha = EncodeUtil.encodeBySha("admin");
        System.out.println(sha);
    }
}