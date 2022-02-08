insert into AZ_TBL (id, dc_id, az_name, active, description) values (1, 1, 'jq-a', 1, 'zone for dc:jq zone A');
insert into AZ_TBL (id, dc_id, az_name, active, description) values (2, 1, 'jq-b', 1, 'zone for dc:jq zone B');

insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active,az_id, keepercontainer_org_id) values (30,3,'127.1.1.1',7033,1,1,0);
insert into KEEPERCONTAINER_TBL(keepercontainer_id,keepercontainer_dc,keepercontainer_ip,keepercontainer_port,keepercontainer_active,az_id, keepercontainer_org_id) values (31,3,'127.1.1.2',7034,1,1,0);

