package com.ctrip.xpipe.redis.core.utils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author wenchao.meng
 *
 * Oct 9, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class MockTest {
	
	@Mock
	private Person person;

	@InjectMocks
	private House house;
	
	@Test
	public void test(){
		
		System.out.println(person.hashCode());
		System.out.println(house);
	}
	
	
	
	public static interface Person{
		
	}
	
	public static class House{
		
		private Person person;
		public Person getPerson() {
			return person;
		}

		public void setPerson(Person person) {
			System.out.println("setPerson:" + person.hashCode());
			this.person = person;
		}
		
		@Override
		public String toString() {
			return String.format("person:%s", person.hashCode());
		}
	}
}
