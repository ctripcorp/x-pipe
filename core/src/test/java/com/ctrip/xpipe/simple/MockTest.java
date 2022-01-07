package com.ctrip.xpipe.simple;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *
 * Sep 9, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class MockTest {
	
	@SuppressWarnings("rawtypes")
	@Mock
	private List mockList;
	
	@Test
	public void testMock(){
		
		int anyInt = anyInt();
		System.out.println(anyInt);
		when(mockList.get(anyInt)).thenReturn("nihao");
				
		System.out.println(mockList.get(0));
		System.out.println(mockList.get(1));
		
	}

	@Test
	public void testMock1(){
		
		when(mockList.get(0)).thenReturn("nihao");
		
		mockList.get(0);
		verify(mockList).get(0);
		
		verify(mockList).get(0);
		
		mockList.get(0);

		verify(mockList, times(2)).get(0);
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testVerify(){
		
		mockList.add("1");
		mockList.add("1");
		verify(mockList, times(2)).add("1");
		
		verifyNoMoreInteractions(mockList);
	}

}
