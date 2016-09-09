package simpletest;

import com.ctrip.xpipe.redis.console.util.SetOperationUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author shyin
 *
 * Sep 1, 2016
 */
public class SetOperationUtilTest {
	@Test
	public void testOperationUtil() {
		Comparator<Integer> integerComparator = new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				return o1 == o2 ? 0 :  -1;
			}
		};

		SetOperationUtil setOperator = new SetOperationUtil();
		List<Integer> list1 = Arrays.asList(1,2,3,4,5);
		List<Integer> list2 = Arrays.asList(3,4,5,6,7);
		List<Integer> list3 = null;
		List<Integer> list4 = new LinkedList<>();
		
		assertEquals(setOperator.difference(Integer.class, list1, list2, integerComparator),Arrays.asList(1,2));
		
		assertEquals(setOperator.difference(Integer.class, list2, list1, integerComparator),Arrays.asList(6,7));
		
		assertEquals(setOperator.intersection(Integer.class, list2, list1, integerComparator),Arrays.asList(3,4,5));

		assertEquals(setOperator.difference(Integer.class, list1, list3, integerComparator), list1);

		assertEquals(setOperator.difference(Integer.class, list1, list4, integerComparator), list1);

		assertEquals(setOperator.difference(Integer.class, list3, list1, integerComparator), new LinkedList<Integer>());

		assertEquals(setOperator.difference(Integer.class, list4, list1, integerComparator), new LinkedList<Integer>());
	}
}
