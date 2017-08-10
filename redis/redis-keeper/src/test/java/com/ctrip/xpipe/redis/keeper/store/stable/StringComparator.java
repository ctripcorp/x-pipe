package com.ctrip.xpipe.redis.keeper.store.stable;

import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.atomic.AtomicInteger;



/**
 * @author wenchao.meng
 *
 * Sep 12, 2016
 */
public class StringComparator {
	
	private int comparatorSize = 1;
	private Pair<Integer, Integer>[]comparatorInfo;
	
	private final int buffsLength = 1 << 20;
	private String []buffs = new String[ buffsLength ];
	private AtomicInteger buffsIndex = new AtomicInteger();
	

	public StringComparator(){
		this(1);
	}

	@SuppressWarnings("unchecked")
	public StringComparator(int comparatorSize){
		this.comparatorSize = comparatorSize;
		this.comparatorInfo = new Pair[comparatorSize];
		for(int i=0; i < comparatorSize ; i++){
			comparatorInfo[i] = new Pair<>(0, 0);
		}
	}
	
	public void add(String buff){
		
		int min = minReadIndex();
		int currentWriteIndex = buffsIndex.get();
		
		int realMin = getRealIndex(min);
		int realCurrent = getRealIndex(currentWriteIndex);
				
		
		if(min != currentWriteIndex && realMin == realCurrent){
			throw new IllegalStateException("reading too slow");
		}
		
		int index = getRealIndex(currentWriteIndex);
		buffs[index] = buff;
		buffsIndex.incrementAndGet();
	}
	
	private int minReadIndex(){
		int min = Integer.MAX_VALUE;
		for(Pair<Integer, Integer> info : comparatorInfo){
			int currentReading = info.getKey();
			if(currentReading < min){
				min = info.getKey();
			}
		}
		return min;
	}

	public boolean compare(String current){
		return compare(0, current);
	}

	public boolean compare(int comparatorIndex, String current){
		
		Pair<Integer, Integer> info = comparatorInfo[comparatorIndex];
		if((info.getKey() > buffsIndex.get()) || (info.getKey() == buffsIndex.get() && current.length() > 0)){
			throw new IllegalStateException("read index > write index " + info.getKey() + "," + buffsIndex.get());
		}
		int  buffIndex = getRealIndex(info.getKey());
		int  buffSubIndex = info.getValue();
		String buff = buffs[buffIndex].substring(buffSubIndex);
		
		if(buff.length() > current.length()){
			info.setValue(info.getValue() + current.length());
			return buff.substring(0, current.length()).equals(current);
		}else if(current.length() > buff.length()){
			
			if(!buff.equals(current.subSequence(0, buff.length()))){
				return false;
			}
			comparatorInfo[comparatorIndex] = new Pair<>(info.getKey() + 1, 0);
			return compare(comparatorIndex, current.substring(buff.length()));
		}else{
			comparatorInfo[comparatorIndex] = new Pair<>(info.getKey() + 1, 0);
			return buff.equals(current);
		}
			
	}

	private int getRealIndex(Integer index) {
		return index%buffsLength;
	}
	
	public int getComparatorSize() {
		return comparatorSize;
	}
	
	public static void main(String []argc){

		StringComparator comparator = new StringComparator(2);
		comparator.add("123456");
		System.out.println(comparator.compare("12"));
		System.out.println(comparator.compare("3456"));
		try{
			System.out.println(comparator.compare("7"));
		}catch(Exception e){
		}

		comparator.add("789");
		System.out.println(comparator.compare(1, "123456789"));
		System.out.println(comparator.compare(1, "123456789"));
		
	}
}
