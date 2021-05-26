insert into ZONE_TBL (id,zone_name) values(1,'SHA');
insert into ZONE_TBL (id,zone_name) values(2,'FRA');

insert into DC_TBL (id,zone_id,dc_name,dc_active,dc_description,dc_last_modified_time) values (1,1,'jq',1,'DC:jq','0000000000000000');
insert into DC_TBL (id,zone_id,dc_name,dc_active,dc_description,dc_last_modified_time) values (2,1,'oy',1,'DC:oy','0000000000000000');
insert into DC_TBL (id,zone_id,dc_name,dc_active,dc_description,dc_last_modified_time) values (3,2,'fra',1,'DC:AWS-FRA','0000000000000000');

insert into SETINEL_TBL (setinel_id,dc_id,setinel_address,setinel_description) values(1,1,'127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002','setinel no.1');
insert into SETINEL_TBL (setinel_id,dc_id,setinel_address,setinel_description) values(2,2,'127.0.0.1:17170,127.0.0.1:17171,127.0.0.1:17172','setinel no.2');
insert into SETINEL_TBL (setinel_id,dc_id,setinel_address,setinel_description) values(3,3,'127.0.0.1:32222,127.0.0.1:32223','setinel no.3');

insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (1,1,'127.0.0.1',7080,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (2,1,'127.0.0.1',7081,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (3,1,'127.0.0.1',7082,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (4,2,'127.0.0.1',7180,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (5,2,'127.0.0.1',7181,1);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active) values (6,2,'127.0.0.1',7182,1);
