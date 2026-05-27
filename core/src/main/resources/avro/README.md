# Avro 协议优化与生成指南

本项目基于 Avro 序列化框架，针对字段类型和编码逻辑进行了性能优化，并提供了 Maven 插件配置方式，方便在编译期自动生成 Java 类。

## 目录

- [1. 使用方法](#1-使用方法)
- [2. 字段类型优化](#2-字段类型优化)
- [3. 编码逻辑优化](#3-编码逻辑优化)

---

## 1. 使用方法

通过 Maven 插件在编译期自动生成 Avro 类：

```xml
<plugin>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro-maven-plugin</artifactId>
    <version>1.11.1</version>
    <executions>
        <execution>
            <id>generate-avro</id>
            <phase>generate-sources</phase>
            <goals>
                <goal>schema</goal>
            </goals>
            <configuration>
                <!-- Avro Schema 文件目录 -->
                <sourceDirectory>src/main/resources/avro/</sourceDirectory>
                <!-- 生成 Java 类的输出目录 -->
                <outputDirectory>${project.basedir}/generated-sources/avro/</outputDirectory>
            </configuration>
        </execution>
    </executions>
</plugin>
```


## 2. 字段类型优化

### 问题
- 使用 `List<Integer>` 存储整型数据时，每次读写都涉及自动装箱（int → Integer）和拆箱（Integer → int）。
- 每个 `Integer` 对象额外占用堆内存（对象头 + 引用），增加 GC 压力。

### 优化方案
将 `List<Integer>` 类型的字段改为原始类型数组 `int[]`。

```java
// 优化前
private List<Integer> key;
private List<Integer> subkey;

// 优化后
private int[] key;
private int[] subkey;
```


 ## 3. 编码逻辑优化
Encode 优化：批量 Buffer 写入与自定义数组序列化

自定义序列化（encode）过程中如何通过 **批量内存缓冲** 和 **高效数组写入** 提升性能，适用于高频 IO 或网络传输场景。

---

在 `customEncode(rg.apache.avro.io.Encoder out)` 方法中，若采用逐字段调用 `writeInt()` 等方式直接写入输出流，会导致：
- 尤其在循环写入大量 `int` 值时，开销显著

---

### 核心思想
1. **先写内存缓冲区**：使用 `buffer` 作为临时缓冲，避免直接写目标流。
2. **批量数组写入**：实现 `writeIntArrayClosed` 方法，一次性将整个 `int[]` 按照约定格式写入缓冲区。
3. **最终一次性刷新**：将缓冲区内容整G体写出到目标 `org.apache.avro.io.Encoder`。

### 优化后的实现

```java
public void customEncode(org.apache.avro.io.Encoder out) throws IOException {
    // 批量写入两个 int 数组
    writeIntArrayClosed(out, this.key);
    writeIntArrayClosed(out, this.subkey);
}
```
