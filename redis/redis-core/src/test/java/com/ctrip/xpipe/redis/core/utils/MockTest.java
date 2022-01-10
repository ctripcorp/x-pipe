package com.ctrip.xpipe.redis.core.utils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

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
	
	
	@Test
	public void testVerify(){
		
		person.getName();
		person.getName();
		
		verify(person, atLeast(1)).getName();
		
		verifyNoMoreInteractions(person);
	}
	
	
	public static interface Person{
		
		String getName();
		
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
