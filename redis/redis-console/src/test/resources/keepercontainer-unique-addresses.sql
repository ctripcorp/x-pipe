-- 测试基础数据历史上让多个 keepercontainer 共用 127.0.0.1，违反一个容器实例一个地址的约束。
update KEEPERCONTAINER_TBL set keepercontainer_ip = '127.0.0.11' where keepercontainer_id = 1;
update KEEPERCONTAINER_TBL set keepercontainer_ip = '127.0.0.12' where keepercontainer_id = 2;
update KEEPERCONTAINER_TBL set keepercontainer_ip = '127.0.0.13' where keepercontainer_id = 3;
update KEEPERCONTAINER_TBL set keepercontainer_ip = '127.0.0.14' where keepercontainer_id = 4;
update KEEPERCONTAINER_TBL set keepercontainer_ip = '127.0.0.15' where keepercontainer_id = 5;
update KEEPERCONTAINER_TBL set keepercontainer_ip = '127.0.0.16' where keepercontainer_id = 6;
