package com.ctrip.xpipe.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * @author wenchao.meng
 *         <p>
 *         2016年3月28日 下午6:46:26
 */
public class StringUtil {

    public static String makeSimpleName(String first, String second){

        String desc;
        if(first == null && second == null){
            desc = "[null]";
        }
        if(first == null){
            desc = second;
        }else if(second == null){
            desc = first;
        }else if(second.startsWith(first)){
            desc = second;
        }else {
            desc = first+ "." +second;
        }
        return "[" + desc + "]";
    }

    public static String subHead(String str, int maxLen){

        if(str == null || str.length() <= maxLen){
            return str;
        }

        return str.substring(0, maxLen);

    }

    public static String toString(Object obj) {
        if (obj == null) {
            return "null";
        }

        StringBuilder sb = new StringBuilder();

        deepToString(obj, sb);
        return sb.toString();
    }

    public static String randomString(int length) {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + (int) (26 * Math.random())));
        }

        return sb.toString();
    }

    public static String join(String split, String... args) {

        return join(split, (arg) -> arg, args);
    }

    public static String join(String split, Object... args) {
        return join(split, (obj) -> obj == null? "null" : obj.toString(), args);
    }

    public static <T> String join(String split, Function<T, String> function, Collection<T> args) {
        StringBuilder sb = new StringBuilder();
        int i = 0;

        for (T arg : args) {
            if (i > 0) {
                sb.append(split);
            }
            if (arg != null) {
                sb.append(function.apply(arg));
            }else {
                sb.append("null");
            }
            i++;
        }
        return sb.toString();
    }

    public static <T> String join(String split, Function<T, String> function, T ... args) {
        StringBuilder sb = new StringBuilder();
        int i = 0;

        for (T arg : args) {
            if (i > 0) {
                sb.append(split);
            }
            if (arg != null) {
                sb.append(function.apply(arg));
            }else {
                sb.append("null");
            }
            i++;
        }
        return sb.toString();
    }

    public static boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    public static String[] splitRemoveEmpty(String regex, String str) {

        String[] temp = str.split(regex);
        List<String> result = new ArrayList<>(temp.length);

        for (String each : temp) {
            if (isEmpty(each)) {
                continue;
            }
            result.add(each);
        }
        return result.toArray(new String[0]);
    }

    public static boolean trimEquals(String str1, String str2, boolean ignoreCase) {

        if (str1 == null) {
            return str2 == null;
        }

        if (str2 == null) {
            return false;
        }

        str1 = str1.trim();
        str2 = str2.trim();

        if (ignoreCase) {
            return str1.equalsIgnoreCase(str2);
        }
        return str1.equals(str2);
    }

    public static boolean trimEquals(String str1, String str2) {
        return trimEquals(str1, str2, false);
    }

    public static String[] splitByLen(String buff, int splitLen) {

        int count = buff.length() / splitLen;
        if (buff.length() % splitLen > 0) {
            count++;
        }
        String[] result = new String[count];
        int index = 0;
        for (int i = 0; i < count; i++) {
            result[i] = buff.substring(index, Math.min(index + splitLen, buff.length()));
            index += splitLen;
        }
        return result;
    }

    private static void deepToString(Object obj, StringBuilder sb) {

        Class<?> oClass = obj.getClass();

        if (obj.getClass().isArray()) {
            if (oClass == byte[].class) {
                sb.append(Arrays.toString((byte[]) obj));
            } else if (oClass == short[].class) {
                sb.append(Arrays.toString((short[]) obj));
            } else if (oClass == int[].class) {
                sb.append(Arrays.toString((int[]) obj));
            } else if (oClass == long[].class) {
                sb.append(Arrays.toString((long[]) obj));
            } else if (oClass == float[].class) {
                sb.append(Arrays.toString((float[]) obj));
            } else if (oClass == double[].class) {
                sb.append(Arrays.toString((double[]) obj));
            } else if (oClass == boolean[].class) {
                sb.append(Arrays.toString((boolean[]) obj));
            } else if (oClass == char[].class) {
                sb.append(Arrays.toString((char[]) obj));
            } else {
                Object[] array = (Object[]) obj;
                sb.append("[");
                int index = 0;
                for (Object ele : array) {
                    deepToString(ele, sb);
                    index++;
                    if (index < array.length) {
                        sb.append(", ");
                    }
                }
                sb.append("]");
            }
        } else {
            sb.append(obj.toString());
        }
    }

    public static int compareVersion(String version1, String version2) {
        String[] v1 = version1.split("\\.");
        String[] v2 = version2.split("\\.");
        for(int i = 0; i < Math.max(v1.length, v2.length); i++) {
            int gap = (i < v1.length ? Integer.parseInt(v1[i]) : 0) - (i < v2.length ? Integer.parseInt(v2[i]) : 0);
            if(gap != 0)    return gap > 0 ? 1 : -1;
        }
        return 0;
    }

    private static final String LINE_SPLITTER = "\\r?\\n";
    private static final String COMMA_SPLITTER = "\\s*,\\s*";

    public static String[] splitByLineRemoveEmpty(String str) {
        return splitRemoveEmpty(LINE_SPLITTER, str);
    }

    public static String[] splitByCommaRemoveEmpty(String str) {
        return splitRemoveEmpty(COMMA_SPLITTER, str);
    }

    public static boolean contains(String str, String subStr) {
        if (str == null || subStr == null) {
            return false;
        }

        int subStrLen = subStr.length();

        if (subStrLen == 0) {
            return true;
        }

        return str.contains(subStr);
    }

}
