
#### 使用示例

```bash
python parser.py [选项] <输入路径>
```

##### **参数说明**

*   **`input_path`** (必需)
    *   描述：要解析的目标路径。
    *   类型：
        *   **文件路径**: 指向单个 `index_` 文件的路径。
        *   **目录路径**: 指向包含一个或多个 `index_` 文件的目录。脚本会自动查找并按文件名排序后依次处理。

*   **`--entry N`** (可选)
    *   描述：只解析并显示指定序号的 `IndexEntry` 记录。序号从 **1** 开始计数。
    *   示例：`--entry 3` 表示只关心第 3 条记录。
    *   如果省略此参数，将默认显示文件中的所有记录。

*   **`--no-block`** (可选)
    *   描述：一个开关选项。如果使用此选项，脚本将**只解析 `index_` 索引文件**，不会尝试读取和解析关联的 `block_` 文件。
    *   用途：
        *   **简化输出**: 当你只关心索引的元数据（如 GNO 范围）时，使用此选项可以获得更简洁的输出。

#### 5. 使用示例

假设 Keeper 的数据文件存储在 `/opt/data/100004376/rsd/replication_store_6480/repl_787907/7ed31054-43f7-4f33-98aa-1f6f78ade1f3/` 目录下。

**示例 1：解析目录中的所有索引文件，不解析block**
```bash
python parser.py  /opt/data/100004376/rsd/replication_store_6480/repl_787907/7ed31054-43f7-4f33-98aa-1f6f78ade1f3/ --no-block
```
> 这会解析 `/opt/data/100004376/rsd/replication_store_6480/repl_787907/7ed31054-43f7-4f33-98aa-1f6f78ade1f3/` 目录下所有的 `index_*` 文件。

**示例 2：解析指定的索引文件，不解析block**
```bash
python parser.py  /opt/data/100004376/rsd/replication_store_6480/repl_787907/7ed31054-43f7-4f33-98aa-1f6f78ade1f3/index_cmd_ca73e222-28e2-4477-a485-d366d0692130_145655346072 --no-block
```
> 这会解析 `index_cmd_ca73e222-28e2-4477-a485-d366d0692130_145655346072` 这个索引文件。

**示例 3：解析指定的索引文件，并指定Entry, 并解析block**
```bash
python parser.py  /opt/data/100004376/rsd/replication_store_6480/repl_787907/7ed31054-43f7-4f33-98aa-1f6f78ade1f3/index_cmd_ca73e222-28e2-4477-a485-d366d0692130_145655346072 --entry 3
```
> 这会解析 `index_cmd_ca73e222-28e2-4477-a485-d366d0692130_145655346072` 这个索引文件第三个索引块和对应的block信息。block会带有gno, 增量信息和cmd_offset


