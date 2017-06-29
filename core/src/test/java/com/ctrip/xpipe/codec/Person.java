package com.ctrip.xpipe.codec;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 28, 2017
 */
public class Person {

    private SEX sex;
    private int age = 1;

    public Person() {

    }

    public Person(SEX sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public SEX getSex() {
        return sex;
    }

    public void setSex(SEX sex) {
        this.sex = sex;
    }

    public enum SEX {
        MALE,
        FEMALE
    }
}


