package com.ctrip.xpipe.redis.core.store;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *
 * Feb 16, 2017
 */
public class SimpleFunctionTest extends AbstractRedisTest{
	
	@Test
	public void setSetMethodAnotherValue(){
		
		String name = randomString(10);
		Person person = new Person();
		person.setAnother(name);

		
		String personStr = JSON.toJSONString(person);
		
		String personStrRep = personStr.replaceAll("another", "name");
		logger.info("{}\n{}", personStr, personStrRep);
		
		Assert.assertNotEquals(personStr, personStrRep);
		
		Person newPerson = JSON.parseObject(personStrRep, Person.class);
		
		Assert.assertEquals(person, newPerson);
	}
	
	@Test
	public void testEquals(){
		
		String name1 = "abc";
		String name2 = new String("abc");
				
		Person person1 = new Person(name1);
		Person person2 = new Person(name2);

		Assert.assertTrue(EqualsBuilder.reflectionEquals(person1, person2));
		
		name2.hashCode();
		Assert.assertTrue(EqualsBuilder.reflectionEquals(person1, person2));
	}
	
	public static class Person{
		
		private String another;
		
		public Person(){
		}
		
		public Person(String another){
			this.another = another;
		}
		public String getAnother() {
			return another;
		}
		
		public void setAnother(String another) {
			this.another = another;
		}
		
		public void setName(String name){
			this.another = name;
		}
		
		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof Person)){
				return false;
			}
			
			Person other = (Person) obj;
			return ObjectUtils.equals(this.another, other.another);
		}
	}
	
	

}
