package com.ctrip.xpipe.utils;

import java.util.regex.Pattern;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 23, 2018
 */
public class VersionUtils {

    private final static Pattern V_SEP = Pattern.compile("[-_./;:\\s]+");


    public static Version parse(String version) {

        String[] split = V_SEP.split(version);
        return new Version(
                split.length >= 1 ? Integer.parseInt(split[0]) : 0,
                split.length >= 2 ? Integer.parseInt(split[1]) : 0,
                split.length >= 3 ? Integer.parseInt(split[2]) : 0
                );
    }

    public static boolean equal(String version1, String version2) {
        Version v1 = parse(version1);
        Version v2 = parse(version2);
        return v1.equals(v2);

    }

    /**
     * version1 > version2
     * @param version1
     * @param version2
     * @return
     */
    public static boolean gt(String version1, String version2) {

        Version v1 = parse(version1);
        Version v2 = parse(version2);
        return v1.compareTo(v2) > 0;

    }

    /**
     * version1 >= version2
     * @param version1
     * @param version2
     * @return
     */
    public static boolean ge(String version1, String version2) {

        Version v1 = parse(version1);
        Version v2 = parse(version2);
        return v1.compareTo(v2) >= 0;

    }


    public static class Version implements Comparable<Version> {

        private int major;
        private int minor;
        private int patch;

        public Version(int major, int minor, int patch) {
            this.major = major;
            this.minor = minor;
            this.patch = patch;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null) return false;
            if (o.getClass() != getClass()) return false;
            Version other = (Version) o;
            return (other.major == major)
                    && (other.minor == minor)
                    && (other.patch == patch);
        }

        @Override
        public int hashCode() {
            return ObjectUtils.hashCode(major, minor, patch);
        }

        @Override
        public int compareTo(Version other) {

            if (this.equals(other)) {
                return 0;
            }
            int diff = 0;
            diff = major - other.major;
            if (diff == 0) {
                diff = minor - other.minor;
                if (diff == 0) {
                    diff = patch - other.patch;
                }
            }
            return diff;
        }

        public int getMajor() {
            return major;
        }

        public int getMinor() {
            return minor;
        }

        public int getPatch() {
            return patch;
        }
    }

}
