insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (1,3,'PROXYTCP://172.19.0.20:80',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (2,1,'PROXYTLS://172.19.0.21:443',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (3,3,'PROXYTCP://172.19.0.22:80',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (4,1,'PROXYTCP://172.19.0.23:443',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (5,1,'PROXYTCP://172.19.0.24:443',1,0,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (6,2,'PROXYTCP://172.19.0.25:443',1,1,0);
insert into proxy_tbl (id,dc_id,uri,active,monitor_active,deleted) values (7,2,'PROXYTCP://172.19.0.26:443',1,1,0);

insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (1, 3, 3, 1, '1,3', '2', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, optional_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (2, 2, 3, 1, '3', '6', '2', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, optional_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (3, 8, 3, 1, '', '', '4', 1, 'console', 0, 'console 使用');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (4, 4, 3, 1, '', '5', 1, 'console', 1, 'console 使用');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (5, 2, 3, 2, '3', '6', 0, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (6, 8, 3, 2, '1', '7', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (7, 7, 3, 2, '1', '6', 1, 'meta', 0, 'desc');

insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (8, 0, 3, 1, '1,3', '2', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, optional_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (9, 0, 3, 1, '3', '6', '2', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, optional_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (10, 0, 3, 1, '', '', '4', 1, 'console', 0, 'console 使用');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (11, 0, 3, 1, '', '5', 1, 'meta', 1, 'console 使用');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (12, 0, 3, 2, '3', '6', 0, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (13, 0, 3, 2, '1', '7', 1, 'meta', 0, 'desc');
insert into route_tbl(id, route_org_id, src_dc_id, dst_dc_id, src_proxy_ids, dst_proxy_ids, active, tag, deleted, description) values (14, 0, 3, 2, '1', '6', 1, 'meta', 0, 'desc');
