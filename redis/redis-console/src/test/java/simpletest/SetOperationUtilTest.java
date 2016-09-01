package simpletest;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import com.ctrip.xpipe.redis.console.util.SetOperationUtil;

/**
 * @author shyin
 *
 * Sep 1, 2016
 */
public class SetOperationUtilTest {
	@Test
	public void testOperationUtil() {
		SetOperationUtil setOperator = new SetOperationUtil();
		List<Integer> list1 = Arrays.asList(1,2,3,4,5);
		List<Integer> list2 = Arrays.asList(3,4,5,6,7);
		
		assertEquals(setOperator.difference(Integer.class, list1, list2, new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				if(o1 == o2) return 0;
				return -1;
			}
		}),Arrays.asList(1,2));
		
		assertEquals(setOperator.difference(Integer.class, list2, list1, new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				if(o1 == o2) return 0;
				return -1;
			}
		}),Arrays.asList(6,7));
		
		assertEquals(setOperator.intersection(Integer.class, list2, list1, new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				if(o1 == o2) return 0;
				return -1;
			}
		}),Arrays.asList(3,4,5));
	}
}
