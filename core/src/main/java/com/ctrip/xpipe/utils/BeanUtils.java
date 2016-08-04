package com.ctrip.xpipe.utils;


import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.util.CollectionUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class BeanUtils {

  /**
   * <pre>
   *     List<UserBean> userBeans = userDao.queryUsers();
   *     List<UserDTO> userDTOs = BeanUtil.batchTransform(UserDTO.class, userBeans);
   * </pre>
   */
  public static <T> List<T> batchTransform(final Class<T> clazz, List<? extends Object> srcList) {
    if (CollectionUtils.isEmpty(srcList)) {
      return Collections.emptyList();
    }

    List<T> result = new ArrayList<>(srcList.size());
    for (Object srcObject : srcList) {
      result.add(transfrom(clazz, srcObject));
    }
    return result;
  }

  /**
   * 封装{@link org.springframework.beans.BeanUtils#copyProperties}，惯用与直接将转换结果返回
   *
   * <pre>
   *      UserBean userBean = new UserBean("username");
   *      return BeanUtil.transform(UserDTO.class, userBean);
   * </pre>
   */
  public static <T> T transfrom(Class<T> clazz, Object src) {
    if (src == null) {
      return null;
    }
    T instance = null;
    try {
      instance = clazz.newInstance();
    } catch (Exception e) {
      throw new BeanUtilsException(e);
    }
    org.springframework.beans.BeanUtils.copyProperties(src, instance, getNullPropertyNames(src));
    return instance;
  }

  private static String[] getNullPropertyNames(Object source) {
    final BeanWrapper src = new BeanWrapperImpl(source);
    PropertyDescriptor[] pds = src.getPropertyDescriptors();

    Set<String> emptyNames = new HashSet<String>();
    for (PropertyDescriptor pd : pds) {
      Object srcValue = src.getPropertyValue(pd.getName());
      if (srcValue == null) emptyNames.add(pd.getName());
    }
    String[] result = new String[emptyNames.size()];
    return emptyNames.toArray(result);
  }

  /**
   * 用于将一个列表转换为列表中的对象的某个属性映射到列表中的对象
   *
   * <pre>
   *      List<UserDTO> userList = userService.queryUsers();
   *      Map<Integer, userDTO> userIdToUser = BeanUtil.mapByKey("userId", userList);
   * </pre>
   *
   * @param key 属性名
   */
  @SuppressWarnings("unchecked")
  public static <K, V> Map<K, V> mapByKey(String key, List<? extends Object> list) {
    Map<K, V> map = new HashMap<K, V>();
    if (CollectionUtils.isEmpty(list)) {
      return map;
    }
    try {
      Class<? extends Object> clazz = list.get(0).getClass();
      Field field = deepFindField(clazz, key);
      if (field == null) throw new IllegalArgumentException("Could not find the key");
      field.setAccessible(true);
      for (Object o : list) {
        map.put((K) field.get(o), (V) o);
      }
    } catch (Exception e) {
      throw new BeanUtilsException(e);
    }
    return map;
  }

  /**
   * 根据列表里面的属性聚合
   *
   * <pre>
   *       List<ShopDTO> shopList = shopService.queryShops();
   *       Map<Integer, List<ShopDTO>> city2Shops = BeanUtil.aggByKeyToList("cityId", shopList);
   * </pre>
   */
  @SuppressWarnings("unchecked")
  public static <K, V> Map<K, List<V>> aggByKeyToList(String key, List<? extends Object> list) {
    Map<K, List<V>> map = new HashMap<K, List<V>>();
    if (CollectionUtils.isEmpty(list)) {// 防止外面传入空list
      return map;
    }
    try {
      Class<? extends Object> clazz = list.get(0).getClass();
      Field field = deepFindField(clazz, key);
      if (field == null) throw new IllegalArgumentException("Could not find the key");
      field.setAccessible(true);
      for (Object o : list) {
        K k = (K) field.get(o);
        if (map.get(k) == null) {
          map.put(k, new ArrayList<V>());
        }
        map.get(k).add((V) o);
      }
    } catch (Exception e) {
      throw new BeanUtilsException(e);
    }
    return map;
  }

  /**
   * 用于将一个对象的列表转换为列表中对象的属性集合
   *
   * <pre>
   *     List<UserDTO> userList = userService.queryUsers();
   *     Set<Integer> userIds = BeanUtil.toPropertySet("userId", userList);
   * </pre>
   */
  @SuppressWarnings("unchecked")
  public static <K> Set<K> toPropertySet(String key, List<? extends Object> list) {
    Set<K> set = new HashSet<K>();
    if (CollectionUtils.isEmpty(list)) {// 防止外面传入空list
      return set;
    }
    try {
      Class<? extends Object> clazz = list.get(0).getClass();
      Field field = deepFindField(clazz, key);
      if (field == null) throw new IllegalArgumentException("Could not find the key");
      field.setAccessible(true);
      for (Object o : list) {
        set.add((K)field.get(o));
      }
    } catch (Exception e) {
      throw new BeanUtilsException(e);
    }
    return set;
  }


  private static Field deepFindField(Class<? extends Object> clazz, String key) {
    Field field = null;
    while (!clazz.getName().equals(Object.class.getName())) {
      try {
        field = clazz.getDeclaredField(key);
        if (field != null) {
          break;
        }
      } catch (Exception e) {
        clazz = clazz.getSuperclass();
      }
    }
    return field;
  }

  /**
   * 获取某个对象的某个属性
   */
  public static Object getProperty(Object obj, String fieldName) {
    try {
      Field field = deepFindField(obj.getClass(), fieldName);
      if (field != null) {
        field.setAccessible(true);
        return field.get(obj);
      }
    } catch (Exception e) {
      throw new BeanUtilsException(e);
    }
    return null;
  }

  /**
   * 设置某个对象的某个属性
   */
  public static void setProperty(Object obj, String fieldName, Object value) {
    try {
      Field field = deepFindField(obj.getClass(), fieldName);
      if (field != null) {
        field.setAccessible(true);
        field.set(obj, value);
      }
    } catch (Exception e) {
      throw new BeanUtilsException(e);
    }
  }

  /**
   * 
   * @param source
   * @param target
   */
  public static void copyProperties(Object source, Object target, String... ignoreProperties) {
    org.springframework.beans.BeanUtils.copyProperties(source, target, ignoreProperties);
  }

  /**
   * The copy will ignore <em>BaseEntity</em> field
   *
   * @param source
   * @param target
   */
  public static void copyEntityProperties(Object source, Object target) {
    org.springframework.beans.BeanUtils.copyProperties(source, target, COPY_IGNORED_PROPERTIES);
  }
  
  private static final String[] COPY_IGNORED_PROPERTIES = {"id", "dataChangeCreatedBy", "dataChangeCreatedTime", "dataChangeLastModifiedTime"};
}
