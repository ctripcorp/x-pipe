package com.ctrip.xpipe.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * @author wenchao.meng
 *         <p>
 *         2016年3月28日 下午6:46:26
 */
public class StringUtil {

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

    public static <T> String join(String split, Function<T, String> function, List<T> args) {
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
        return str == null || str.trim().length() == 0;
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
        return str1.equalsIgnoreCase(str2);
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
}
