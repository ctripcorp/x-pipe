package com.ctrip.xpipe.codec;

import com.ctrip.xpipe.utils.ObjectUtils;

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

    public Person(SEX sex, int age) {
        this.sex = sex;
        this.age = age;
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

    @Override
    public String toString() {
        return String.format("sex:" + sex + ",age:" + age);
    }


    @Override
    public boolean equals(Object obj) {

        if(!(obj instanceof Person)){
            return false;
        }

        Person other = (Person) obj;

        if(!(ObjectUtils.equals(sex, other.sex))){
            return false;
        }

        if(!(ObjectUtils.equals(age, other.age))){
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(age, sex);
    }
}


