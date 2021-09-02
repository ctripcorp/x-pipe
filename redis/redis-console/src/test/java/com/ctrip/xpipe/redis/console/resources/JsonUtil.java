package com.ctrip.xpipe.redis.console.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static final String xpipeDcMeta="{\n" +
            "    \"clusters\": {\n" +
            "        \"xpipe_function\": {\n" +
            "            \"dcs\": null,\n" +
            "            \"orgId\": 8,\n" +
            "            \"activeDc\": \"NTGXH\",\n" +
            "            \"shards\": {\n" +
            "                \"shard1\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.55.173\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.25.214\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"xpipe_function+xpipe_function-shard1+NTGXH\",\n" +
            "                    \"keepers\": [\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 2,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.48.237\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"c639714d94aaba161c449fb749fe6d8159201f74\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 1,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.42.209\",\n" +
            "                            \"port\": 6450,\n" +
            "                            \"id\": \"c639714d94aaba161c449fb749fe6d8159201f74\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"shard1\"\n" +
            "                },\n" +
            "                \"shard2\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.55.173\",\n" +
            "                            \"port\": 6479,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.25.214\",\n" +
            "                            \"port\": 6479,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"xpipe_function+xpipe_function-shard2+NTGXH\",\n" +
            "                    \"keepers\": [\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 2,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.48.237\",\n" +
            "                            \"port\": 6381,\n" +
            "                            \"id\": \"13ed1e1a51caf7491cb52947eac371b5ef4ccef7\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 11,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.5.109.218\",\n" +
            "                            \"port\": 6543,\n" +
            "                            \"id\": \"13ed1e1a51caf7491cb52947eac371b5ef4ccef7\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"shard2\"\n" +
            "                },\n" +
            "                \"shard3\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.55.173\",\n" +
            "                            \"port\": 6579,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.25.214\",\n" +
            "                            \"port\": 6579,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"xpipe_function+xpipe_function-shard3+NTGXH\",\n" +
            "                    \"keepers\": [\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 2,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.48.237\",\n" +
            "                            \"port\": 6388,\n" +
            "                            \"id\": \"740c580236c93c10e328665508a504f1127a113d\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 1,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.42.209\",\n" +
            "                            \"port\": 6452,\n" +
            "                            \"id\": \"740c580236c93c10e328665508a504f1127a113d\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"shard3\"\n" +
            "                },\n" +
            "                \"shard4\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.55.173\",\n" +
            "                            \"port\": 6679,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.25.214\",\n" +
            "                            \"port\": 6679,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"xpipe_function+xpipe_function-shard4+NTGXH\",\n" +
            "                    \"keepers\": [\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 1,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.42.209\",\n" +
            "                            \"port\": 6386,\n" +
            "                            \"id\": \"5e86224ff3a2ee3efded105ce8c98e3e27677217\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 2,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.48.237\",\n" +
            "                            \"port\": 6393,\n" +
            "                            \"id\": \"5e86224ff3a2ee3efded105ce8c98e3e27677217\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"shard4\"\n" +
            "                },\n" +
            "                \"shard5\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.55.173\",\n" +
            "                            \"port\": 6779,\n" +
            "                            \"id\": \"20180118165508724-20180118165508723-unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.25.214\",\n" +
            "                            \"port\": 6779,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"xpipe_function+xpipe_functionshard5+NTGXH\",\n" +
            "                    \"keepers\": [\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 1,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.42.209\",\n" +
            "                            \"port\": 6388,\n" +
            "                            \"id\": \"b20d11e632d4a82cb96ba1ceaafd7d8b10d1b840\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 7,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.24.217\",\n" +
            "                            \"port\": 6399,\n" +
            "                            \"id\": \"b20d11e632d4a82cb96ba1ceaafd7d8b10d1b840\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"shard5\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": \"UAT\",\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"one_way\",\n" +
            "            \"id\": \"xpipe_function\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"xpipe-test\": {\n" +
            "            \"dcs\": null,\n" +
            "            \"orgId\": 44,\n" +
            "            \"activeDc\": \"NTGXH\",\n" +
            "            \"shards\": {\n" +
            "                \"shard-test\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.24.217\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.24.218\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"xpipe-test+xpipe-test-shard-test+NTGXH\",\n" +
            "                    \"keepers\": [\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 8,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.24.218\",\n" +
            "                            \"port\": 6380,\n" +
            "                            \"id\": \"09e2842b560b184e2e4e834a4b0777658af5fa84\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 1,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.42.209\",\n" +
            "                            \"port\": 5000,\n" +
            "                            \"id\": \"09e2842b560b184e2e4e834a4b0777658af5fa84\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"shard-test\"\n" +
            "                },\n" +
            "                \"shard-test2\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.24.217\",\n" +
            "                            \"port\": 16379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"xpipe-test+xpipe-testshard-test2+NTGXH\",\n" +
            "                    \"keepers\": [\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 8,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.24.218\",\n" +
            "                            \"port\": 6381,\n" +
            "                            \"id\": \"73bf92d7f99b74c8c859b0c05050efe03eaede90\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 11,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.5.109.218\",\n" +
            "                            \"port\": 6412,\n" +
            "                            \"id\": \"73bf92d7f99b74c8c859b0c05050efe03eaede90\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"shard-test2\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": \"UAT\",\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"one_way\",\n" +
            "            \"id\": \"xpipe-test\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"HTL_KeyCache_Push_RB\": {\n" +
            "            \"dcs\": \"NTGXH,UAT\",\n" +
            "            \"orgId\": 9,\n" +
            "            \"activeDc\": null,\n" +
            "            \"shards\": {\n" +
            "                \"HTL_KeyCache_Push_RB_1\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.254.65\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.219.96\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"HTL_KeyCache_Push_RB+HTL_KeyCache_Push_RB_1+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"HTL_KeyCache_Push_RB_1\"\n" +
            "                },\n" +
            "                \"HTL_KeyCache_Push_RB_2\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.22.27.226\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.221.31\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"HTL_KeyCache_Push_RB+HTL_KeyCache_Push_RB_2+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"HTL_KeyCache_Push_RB_2\"\n" +
            "                },\n" +
            "                \"HTL_KeyCache_Push_RB_3\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.221.247\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.22.27.124\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"HTL_KeyCache_Push_RB+HTL_KeyCache_Push_RB_3+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"HTL_KeyCache_Push_RB_3\"\n" +
            "                },\n" +
            "                \"HTL_KeyCache_Push_RB_4\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.219.248\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.235.138\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"HTL_KeyCache_Push_RB+HTL_KeyCache_Push_RB_4+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"HTL_KeyCache_Push_RB_4\"\n" +
            "                },\n" +
            "                \"HTL_KeyCache_Push_RB_5\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.249.13\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.235.117\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"HTL_KeyCache_Push_RB+HTL_KeyCache_Push_RB_5+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"HTL_KeyCache_Push_RB_5\"\n" +
            "                },\n" +
            "                \"HTL_KeyCache_Push_RB_6\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.22.27.62\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.222.143\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"HTL_KeyCache_Push_RB+HTL_KeyCache_Push_RB_6+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"HTL_KeyCache_Push_RB_6\"\n" +
            "                },\n" +
            "                \"HTL_KeyCache_Push_RB_7\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.252.124\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.222.134\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"HTL_KeyCache_Push_RB+HTL_KeyCache_Push_RB_7+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"HTL_KeyCache_Push_RB_7\"\n" +
            "                },\n" +
            "                \"HTL_KeyCache_Push_RB_8\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.222.127\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.22.27.242\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"HTL_KeyCache_Push_RB+HTL_KeyCache_Push_RB_8+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"HTL_KeyCache_Push_RB_8\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": null,\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"bi_direction\",\n" +
            "            \"id\": \"HTL_KeyCache_Push_RB\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"test_auto_creation_cluster\": {\n" +
            "            \"dcs\": \"NTGXH,UAT\",\n" +
            "            \"orgId\": 18,\n" +
            "            \"activeDc\": null,\n" +
            "            \"shards\": {\n" +
            "                \"test_auto_creation_cluster_1\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.254.226\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.223.209\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"test_auto_creation_cluster+test_auto_creation_cluster_1+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"test_auto_creation_cluster_1\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": null,\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"bi_direction\",\n" +
            "            \"id\": \"test_auto_creation_cluster\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"test_xredis_1.0.10\": {\n" +
            "            \"dcs\": \"NTGXH,UAT\",\n" +
            "            \"orgId\": 18,\n" +
            "            \"activeDc\": null,\n" +
            "            \"shards\": {\n" +
            "                \"test_xredis_1.0.10_1\": {\n" +
            "                    \"sentinelId\": 1,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.253.250\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.223.243\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"test_xredis_1.0.10+test_xredis_1.0.10_1+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"test_xredis_1.0.10_1\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": null,\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"bi_direction\",\n" +
            "            \"id\": \"test_xredis_1.0.10\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"test_xredis_1.0.11\": {\n" +
            "            \"dcs\": \"NTGXH,UAT\",\n" +
            "            \"orgId\": 18,\n" +
            "            \"activeDc\": null,\n" +
            "            \"shards\": {\n" +
            "                \"test_xredis_1.0.11_1\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.216.198\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.219.37\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"test_xredis_1.0.11+test_xredis_1.0.11_1+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"test_xredis_1.0.11_1\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": null,\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"bi_direction\",\n" +
            "            \"id\": \"test_xredis_1.0.11\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"test_xredis_1.0.13\": {\n" +
            "            \"dcs\": \"UAT,NTGXH\",\n" +
            "            \"orgId\": 18,\n" +
            "            \"activeDc\": null,\n" +
            "            \"shards\": {\n" +
            "                \"test_xredis_1.0.13_1\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.239.86\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.219.200\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"test_xredis_1.0.13+test_xredis_1.0.13_1+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"test_xredis_1.0.13_1\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": null,\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"bi_direction\",\n" +
            "            \"id\": \"test_xredis_1.0.13\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"test_xredis_1.0.12\": {\n" +
            "            \"dcs\": \"NTGXH,UAT\",\n" +
            "            \"orgId\": 18,\n" +
            "            \"activeDc\": null,\n" +
            "            \"shards\": {\n" +
            "                \"test_xredis_1.0.12_1\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.22.125.210\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.22.27.179\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"test_xredis_1.0.12+test_xredis_1.0.12_1+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"test_xredis_1.0.12_1\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": null,\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"bi_direction\",\n" +
            "            \"id\": \"test_xredis_1.0.12\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"test_crdt_migrate\": {\n" +
            "            \"dcs\": \"NTGXH,UAT\",\n" +
            "            \"orgId\": 18,\n" +
            "            \"activeDc\": null,\n" +
            "            \"shards\": {\n" +
            "                \"test_crdt_migrate_2\": {\n" +
            "                    \"sentinelId\": 1,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.221.194\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.222.154\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"test_crdt_migrate+test_crdt_migrate_2+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"test_crdt_migrate_2\"\n" +
            "                },\n" +
            "                \"test_crdt_migrate_1\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.222.32\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.221.181\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"test_crdt_migrate+test_crdt_migrate_1+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"test_crdt_migrate_1\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": null,\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"bi_direction\",\n" +
            "            \"id\": \"test_crdt_migrate\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"HotelAllianceDataChangeCache\": {\n" +
            "            \"dcs\": null,\n" +
            "            \"orgId\": 9,\n" +
            "            \"activeDc\": \"UAT\",\n" +
            "            \"shards\": {\n" +
            "                \"HotelAllianceDataChangeCache_1\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.22.76.64\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"HotelAllianceDataChangeCache+HotelAllianceDataChangeCache_1+NTGXH\",\n" +
            "                    \"keepers\": [\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 1,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.42.209\",\n" +
            "                            \"port\": 6239,\n" +
            "                            \"id\": \"9013a135ac83fd856677bb218c28003266d2e5f5\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 2,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.48.237\",\n" +
            "                            \"port\": 6296,\n" +
            "                            \"id\": \"9013a135ac83fd856677bb218c28003266d2e5f5\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"HotelAllianceDataChangeCache_1\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": \"NTGXH\",\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"one_way\",\n" +
            "            \"id\": \"HotelAllianceDataChangeCache\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"test_xpipe_one_way_migrate\": {\n" +
            "            \"dcs\": \"UAT,NTGXH\",\n" +
            "            \"orgId\": 7,\n" +
            "            \"activeDc\": null,\n" +
            "            \"shards\": {\n" +
            "                \"test_xpipe_one_way_migrate_2\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.4.73.140\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.4.66.236\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"test_xpipe_one_way_migrate+test_xpipe_one_way_migrate_2+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"test_xpipe_one_way_migrate_2\"\n" +
            "                },\n" +
            "                \"test_xpipe_one_way_migrate_3\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.4.73.135\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.4.66.185\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"test_xpipe_one_way_migrate+test_xpipe_one_way_migrate_3+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"test_xpipe_one_way_migrate_3\"\n" +
            "                },\n" +
            "                \"test_xpipe_one_way_migrate_4\": {\n" +
            "                    \"sentinelId\": 1,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.4.73.173\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.4.66.254\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"test_xpipe_one_way_migrate+test_xpipe_one_way_migrate_4+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"test_xpipe_one_way_migrate_4\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": null,\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"bi_direction\",\n" +
            "            \"id\": \"test_xpipe_one_way_migrate\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"test_one_way_migrate2\": {\n" +
            "            \"dcs\": null,\n" +
            "            \"orgId\": 8,\n" +
            "            \"activeDc\": \"NTGXH\",\n" +
            "            \"shards\": {\n" +
            "                \"test_one_way_migrate2_1\": {\n" +
            "                    \"sentinelId\": 3,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.253.95\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.223.218\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"test_one_way_migrate2+test_one_way_migrate2_1+NTGXH\",\n" +
            "                    \"keepers\": [\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 8,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.24.218\",\n" +
            "                            \"port\": 6383,\n" +
            "                            \"id\": \"541eb3921ba3ad734a3c17498d4ed70be51eda49\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": null,\n" +
            "                            \"active\": false,\n" +
            "                            \"keeperContainerId\": 2,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.2.48.237\",\n" +
            "                            \"port\": 6395,\n" +
            "                            \"id\": \"541eb3921ba3ad734a3c17498d4ed70be51eda49\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"test_one_way_migrate2_1\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": \"UAT\",\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"one_way\",\n" +
            "            \"id\": \"test_one_way_migrate2\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"LocalDcCacheCluster\": {\n" +
            "            \"dcs\": \"NTGXH,UAT\",\n" +
            "            \"orgId\": 9,\n" +
            "            \"activeDc\": null,\n" +
            "            \"shards\": {\n" +
            "                \"credis_test_cluster_3_1\": {\n" +
            "                    \"sentinelId\": 0,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.21.179\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.7.159\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"LocalDcCacheCluster+credis_test_cluster_3_1+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"credis_test_cluster_3_1\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": null,\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"LOCAL_DC\",\n" +
            "            \"id\": \"LocalDcCacheCluster\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        },\n" +
            "        \"SingleDcCacheCluster\": {\n" +
            "            \"dcs\": null,\n" +
            "            \"orgId\": 18,\n" +
            "            \"activeDc\": \"NTGXH\",\n" +
            "            \"shards\": {\n" +
            "                \"credis_test_cluster_1_1\": {\n" +
            "                    \"sentinelId\": 0,\n" +
            "                    \"redises\": [\n" +
            "                        {\n" +
            "                            \"master\": \"\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.7.111\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        },\n" +
            "                        {\n" +
            "                            \"master\": \"0.0.0.0:0\",\n" +
            "                            \"gid\": null,\n" +
            "                            \"survive\": null,\n" +
            "                            \"ip\": \"10.6.251.113\",\n" +
            "                            \"port\": 6379,\n" +
            "                            \"id\": \"unknown\",\n" +
            "                            \"offset\": null\n" +
            "                        }\n" +
            "                    ],\n" +
            "                    \"sentinelMonitorName\": \"SingleDcCacheCluster+credis_test_cluster_1_1+NTGXH\",\n" +
            "                    \"keepers\": [],\n" +
            "                    \"phase\": null,\n" +
            "                    \"id\": \"credis_test_cluster_1_1\"\n" +
            "                }\n" +
            "            },\n" +
            "            \"backupDcs\": \"\",\n" +
            "            \"adminEmails\": null,\n" +
            "            \"phase\": null,\n" +
            "            \"type\": \"SINGLE_DC\",\n" +
            "            \"id\": \"SingleDcCacheCluster\",\n" +
            "            \"lastModifiedTime\": null\n" +
            "        }\n" +
            "    },\n" +
            "    \"sentinels\": {\n" +
            "        \"1\": {\n" +
            "            \"address\": \"10.2.48.234:5000,10.2.48.234:5001,10.2.48.234:5002,10.2.48.234:5003,10.2.48.234:5004\",\n" +
            "            \"id\": 1\n" +
            "        },\n" +
            "        \"3\": {\n" +
            "            \"address\": \"10.2.38.97:5000,10.2.38.97:5001,10.2.38.97:5002,10.2.38.97:5003,10.2.38.97:5004\",\n" +
            "            \"id\": 3\n" +
            "        },\n" +
            "        \"151\": {\n" +
            "            \"address\": \"10.2.27.105:5004,10.2.27.98:5004,10.2.27.97:5004,10.2.27.96:5004,10.2.27.104:5004\",\n" +
            "            \"id\": 151\n" +
            "        }\n" +
            "    },\n" +
            "    \"keeperContainers\": [\n" +
            "        {\n" +
            "            \"ip\": \"10.2.42.209\",\n" +
            "            \"port\": 8080,\n" +
            "            \"id\": 1\n" +
            "        },\n" +
            "        {\n" +
            "            \"ip\": \"10.2.48.237\",\n" +
            "            \"port\": 8080,\n" +
            "            \"id\": 2\n" +
            "        },\n" +
            "        {\n" +
            "            \"ip\": \"10.2.24.217\",\n" +
            "            \"port\": 8080,\n" +
            "            \"id\": 7\n" +
            "        },\n" +
            "        {\n" +
            "            \"ip\": \"10.2.24.218\",\n" +
            "            \"port\": 8080,\n" +
            "            \"id\": 8\n" +
            "        },\n" +
            "        {\n" +
            "            \"ip\": \"10.5.109.218\",\n" +
            "            \"port\": 8080,\n" +
            "            \"id\": 11\n" +
            "        }\n" +
            "    ],\n" +
            "    \"zkServer\": null,\n" +
            "    \"zone\": \"SHA\",\n" +
            "    \"metaServers\": [],\n" +
            "    \"routes\": [],\n" +
            "    \"id\": \"NTGXH\",\n" +
            "    \"lastModifiedTime\": \"201612051157001\"\n" +
            "}";

    static final String credisMetaString="{\"id\":0,\"dcName\":null,\"regionId\":0,\"sentinels\":{},\"clusters\":{\"LocalDcCacheCluster\":{\"clusterId\":2830,\"name\":\"LocalDcCacheCluster\",\"lastModifiedTime\":\"2020-07-02 14:53:21\",\"rootGroup\":{\"clusterName\":\"LocalDcCacheCluster\",\"groupName\":\"credis_test_cluster_3_ClusterGroup\",\"groupId\":1000000457,\"compatibleGroupId\":0,\"parentGroupId\":0,\"leafNode\":false,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":-1882675799,\"indexInParent\":-1,\"routingStrategy\":\"ketamahashstrategy\",\"subGroups\":[{\"clusterName\":\"LocalDcCacheCluster\",\"groupName\":\"credis_test_cluster_3_1\",\"groupId\":1000000476,\"compatibleGroupId\":0,\"parentGroupId\":1000000457,\"leafNode\":true,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":-102172238,\"indexInParent\":0,\"routingStrategy\":\"modhashstrategy\",\"subGroups\":null,\"redises\":[{\"host\":\"10.6.7.159\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7119,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"},{\"host\":\"10.6.21.179\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7122,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}],\"masters\":{\"credis_test_cluster_3_1\":[{\"host\":\"10.6.21.179\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7122,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"credis_test_cluster_3_1\":[{\"host\":\"10.6.7.159\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7119,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}}],\"redises\":null,\"masters\":{\"credis_test_cluster_3_1\":[{\"host\":\"10.6.21.179\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7122,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"credis_test_cluster_3_1\":[{\"host\":\"10.6.7.159\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7119,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}},\"usingIDC\":1,\"activeIDC\":\"\",\"excludedIdcs\":\"\",\"routeStrategy\":1,\"dbNumber\":0,\"clusterType\":\"LOCAL_DC\",\"ownerEmails\":\"lilj@ctrip.com\",\"orgId\":45,\"groups\":{\"credis_test_cluster_3_1\":{\"clusterName\":\"LocalDcCacheCluster\",\"groupName\":\"credis_test_cluster_3_1\",\"groupId\":1000000476,\"compatibleGroupId\":0,\"parentGroupId\":1000000457,\"leafNode\":true,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":-102172238,\"indexInParent\":0,\"routingStrategy\":\"modhashstrategy\",\"subGroups\":null,\"redises\":[{\"host\":\"10.6.7.159\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7119,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"},{\"host\":\"10.6.21.179\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7122,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}],\"masters\":{\"credis_test_cluster_3_1\":[{\"host\":\"10.6.21.179\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7122,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"credis_test_cluster_3_1\":[{\"host\":\"10.6.7.159\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7119,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}}}},\"xredis_crdt\":{\"clusterId\":3635,\"name\":\"xredis_crdt\",\"lastModifiedTime\":\"2020-07-02 15:13:31\",\"rootGroup\":{\"clusterName\":\"xredis_crdt\",\"groupName\":\"xredis_crdt_ClusterGroup\",\"groupId\":1000002322,\"compatibleGroupId\":0,\"parentGroupId\":0,\"leafNode\":false,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":1092314443,\"indexInParent\":-1,\"routingStrategy\":\"ketamahashstrategy\",\"subGroups\":[{\"clusterName\":\"xredis_crdt\",\"groupName\":\"xredis_crdt_1\",\"groupId\":1000002323,\"compatibleGroupId\":0,\"parentGroupId\":1000002322,\"leafNode\":true,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":-238644664,\"indexInParent\":0,\"routingStrategy\":\"modhashstrategy\",\"subGroups\":null,\"redises\":[{\"host\":\"10.2.37.75\",\"port\":7379,\"master\":true,\"status\":\"ACTIVE\",\"id\":33794,\"idc\":\"NTGXH\",\"canRead\":false,\"dbNumber\":0,\"backStream\":\"available\"},{\"host\":\"10.2.37.77\",\"port\":7379,\"master\":false,\"status\":\"ACTIVE\",\"id\":35225,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}],\"masters\":{\"xredis_crdt_1\":[{\"host\":\"10.2.37.75\",\"port\":7379,\"master\":true,\"status\":\"ACTIVE\",\"id\":33794,\"idc\":\"NTGXH\",\"canRead\":false,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"xredis_crdt_1\":[{\"host\":\"10.2.37.77\",\"port\":7379,\"master\":false,\"status\":\"ACTIVE\",\"id\":35225,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}},{\"clusterName\":\"xredis_crdt\",\"groupName\":\"xredis_crdt_2\",\"groupId\":1000002324,\"compatibleGroupId\":0,\"parentGroupId\":1000002322,\"leafNode\":true,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":-1395255722,\"indexInParent\":1,\"routingStrategy\":\"modhashstrategy\",\"subGroups\":null,\"redises\":[{\"host\":\"10.2.37.75\",\"port\":7479,\"master\":false,\"status\":\"ACTIVE\",\"id\":33898,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"},{\"host\":\"10.2.37.77\",\"port\":7479,\"master\":true,\"status\":\"ACTIVE\",\"id\":35226,\"idc\":\"NTGXH\",\"canRead\":false,\"dbNumber\":0,\"backStream\":\"available\"}],\"masters\":{\"xredis_crdt_2\":[{\"host\":\"10.2.37.77\",\"port\":7479,\"master\":true,\"status\":\"ACTIVE\",\"id\":35226,\"idc\":\"NTGXH\",\"canRead\":false,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"xredis_crdt_2\":[{\"host\":\"10.2.37.75\",\"port\":7479,\"master\":false,\"status\":\"ACTIVE\",\"id\":33898,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}}],\"redises\":null,\"masters\":{\"xredis_crdt_2\":[{\"host\":\"10.2.37.77\",\"port\":7479,\"master\":true,\"status\":\"ACTIVE\",\"id\":35226,\"idc\":\"NTGXH\",\"canRead\":false,\"dbNumber\":0,\"backStream\":\"available\"}],\"xredis_crdt_1\":[{\"host\":\"10.2.37.75\",\"port\":7379,\"master\":true,\"status\":\"ACTIVE\",\"id\":33794,\"idc\":\"NTGXH\",\"canRead\":false,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"xredis_crdt_2\":[{\"host\":\"10.2.37.75\",\"port\":7479,\"master\":false,\"status\":\"ACTIVE\",\"id\":33898,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}],\"xredis_crdt_1\":[{\"host\":\"10.2.37.77\",\"port\":7379,\"master\":false,\"status\":\"ACTIVE\",\"id\":35225,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}},\"usingIDC\":1,\"activeIDC\":\"\",\"excludedIdcs\":\"\",\"routeStrategy\":5,\"dbNumber\":0,\"clusterType\":\"XPIPE_BI_DIRECT\",\"ownerEmails\":\"gd.zhou@trip.com\",\"orgId\":56,\"groups\":{\"xredis_crdt_2\":{\"clusterName\":\"xredis_crdt\",\"groupName\":\"xredis_crdt_2\",\"groupId\":1000002324,\"compatibleGroupId\":0,\"parentGroupId\":1000002322,\"leafNode\":true,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":-1395255722,\"indexInParent\":1,\"routingStrategy\":\"modhashstrategy\",\"subGroups\":null,\"redises\":[{\"host\":\"10.2.37.75\",\"port\":7479,\"master\":false,\"status\":\"ACTIVE\",\"id\":33898,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"},{\"host\":\"10.2.37.77\",\"port\":7479,\"master\":true,\"status\":\"ACTIVE\",\"id\":35226,\"idc\":\"NTGXH\",\"canRead\":false,\"dbNumber\":0,\"backStream\":\"available\"}],\"masters\":{\"xredis_crdt_2\":[{\"host\":\"10.2.37.77\",\"port\":7479,\"master\":true,\"status\":\"ACTIVE\",\"id\":35226,\"idc\":\"NTGXH\",\"canRead\":false,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"xredis_crdt_2\":[{\"host\":\"10.2.37.75\",\"port\":7479,\"master\":false,\"status\":\"ACTIVE\",\"id\":33898,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}},\"xredis_crdt_1\":{\"clusterName\":\"xredis_crdt\",\"groupName\":\"xredis_crdt_1\",\"groupId\":1000002323,\"compatibleGroupId\":0,\"parentGroupId\":1000002322,\"leafNode\":true,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":-238644664,\"indexInParent\":0,\"routingStrategy\":\"modhashstrategy\",\"subGroups\":null,\"redises\":[{\"host\":\"10.2.37.75\",\"port\":7379,\"master\":true,\"status\":\"ACTIVE\",\"id\":33794,\"idc\":\"NTGXH\",\"canRead\":false,\"dbNumber\":0,\"backStream\":\"available\"},{\"host\":\"10.2.37.77\",\"port\":7379,\"master\":false,\"status\":\"ACTIVE\",\"id\":35225,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}],\"masters\":{\"xredis_crdt_1\":[{\"host\":\"10.2.37.75\",\"port\":7379,\"master\":true,\"status\":\"ACTIVE\",\"id\":33794,\"idc\":\"NTGXH\",\"canRead\":false,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"xredis_crdt_1\":[{\"host\":\"10.2.37.77\",\"port\":7379,\"master\":false,\"status\":\"ACTIVE\",\"id\":35225,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}}}},\"SingleDcCacheCluster\":{\"clusterId\":2822,\"name\":\"SingleDcCacheCluster\",\"lastModifiedTime\":\"2020-07-02 14:53:39\",\"rootGroup\":{\"clusterName\":\"SingleDcCacheCluster\",\"groupName\":\"credis_test_cluster_1_ClusterGroup\",\"groupId\":1000000448,\"compatibleGroupId\":0,\"parentGroupId\":0,\"leafNode\":false,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":-508745800,\"indexInParent\":-1,\"routingStrategy\":\"ketamahashstrategy\",\"subGroups\":[{\"clusterName\":\"SingleDcCacheCluster\",\"groupName\":\"credis_test_cluster_1_1\",\"groupId\":1000000468,\"compatibleGroupId\":0,\"parentGroupId\":1000000448,\"leafNode\":true,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":-1485539207,\"indexInParent\":0,\"routingStrategy\":\"modhashstrategy\",\"subGroups\":null,\"redises\":[{\"host\":\"10.6.7.111\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7114,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"},{\"host\":\"10.6.251.113\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7123,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}],\"masters\":{\"credis_test_cluster_1_1\":[{\"host\":\"10.6.7.111\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7114,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"credis_test_cluster_1_1\":[{\"host\":\"10.6.251.113\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7123,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}}],\"redises\":null,\"masters\":{\"credis_test_cluster_1_1\":[{\"host\":\"10.6.7.111\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7114,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"credis_test_cluster_1_1\":[{\"host\":\"10.6.251.113\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7123,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}},\"usingIDC\":0,\"activeIDC\":\"\",\"excludedIdcs\":\"\",\"routeStrategy\":1,\"dbNumber\":0,\"clusterType\":\"SINGEL_DC\",\"ownerEmails\":\"lilj@ctrip.com\",\"orgId\":45,\"groups\":{\"credis_test_cluster_1_1\":{\"clusterName\":\"SingleDcCacheCluster\",\"groupName\":\"credis_test_cluster_1_1\",\"groupId\":1000000468,\"compatibleGroupId\":0,\"parentGroupId\":1000000448,\"leafNode\":true,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":-1485539207,\"indexInParent\":0,\"routingStrategy\":\"modhashstrategy\",\"subGroups\":null,\"redises\":[{\"host\":\"10.6.7.111\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7114,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"},{\"host\":\"10.6.251.113\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7123,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}],\"masters\":{\"credis_test_cluster_1_1\":[{\"host\":\"10.6.7.111\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7114,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"credis_test_cluster_1_1\":[{\"host\":\"10.6.251.113\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7123,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}}}},\"XPipe_ReadActiveDc_Cluster\":{\"clusterId\":2824,\"name\":\"XPipe_ReadActiveDc_Cluster\",\"lastModifiedTime\":\"2020-07-02 14:53:09\",\"rootGroup\":{\"clusterName\":\"XPipe_ReadActiveDc_Cluster\",\"groupName\":\"credis_test_cluster_10_ClusterGroup\",\"groupId\":1000000449,\"compatibleGroupId\":0,\"parentGroupId\":0,\"leafNode\":false,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":1235929418,\"indexInParent\":-1,\"routingStrategy\":\"ketamahashstrategy\",\"subGroups\":[{\"clusterName\":\"XPipe_ReadActiveDc_Cluster\",\"groupName\":\"credis_test_cluster_10_1\",\"groupId\":1000000470,\"compatibleGroupId\":0,\"parentGroupId\":1000000449,\"leafNode\":true,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":631660404,\"indexInParent\":0,\"routingStrategy\":\"modhashstrategy\",\"subGroups\":null,\"redises\":[{\"host\":\"10.6.68.45\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7116,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"},{\"host\":\"10.6.7.39\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7126,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}],\"masters\":{\"credis_test_cluster_10_1\":[{\"host\":\"10.6.68.45\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7116,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"credis_test_cluster_10_1\":[{\"host\":\"10.6.7.39\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7126,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}}],\"redises\":null,\"masters\":{\"credis_test_cluster_10_1\":[{\"host\":\"10.6.68.45\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7116,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"credis_test_cluster_10_1\":[{\"host\":\"10.6.7.39\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7126,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}},\"usingIDC\":1,\"activeIDC\":\"NTGXH\",\"excludedIdcs\":\"\",\"routeStrategy\":2,\"dbNumber\":0,\"clusterType\":\"XPIPE_ONE_WAY\",\"ownerEmails\":\"lilj@ctrip.com\",\"orgId\":44,\"groups\":{\"credis_test_cluster_10_1\":{\"clusterName\":\"XPipe_ReadActiveDc_Cluster\",\"groupName\":\"credis_test_cluster_10_1\",\"groupId\":1000000470,\"compatibleGroupId\":0,\"parentGroupId\":1000000449,\"leafNode\":true,\"sentinelId\":0,\"sentinelMonitorName\":\"\",\"hashMagicInt\":631660404,\"indexInParent\":0,\"routingStrategy\":\"modhashstrategy\",\"subGroups\":null,\"redises\":[{\"host\":\"10.6.68.45\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7116,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"},{\"host\":\"10.6.7.39\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7126,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}],\"masters\":{\"credis_test_cluster_10_1\":[{\"host\":\"10.6.68.45\",\"port\":6379,\"master\":true,\"status\":\"ACTIVE\",\"id\":7116,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]},\"slaves\":{\"credis_test_cluster_10_1\":[{\"host\":\"10.6.7.39\",\"port\":6379,\"master\":false,\"status\":\"ACTIVE\",\"id\":7126,\"idc\":\"NTGXH\",\"canRead\":true,\"dbNumber\":0,\"backStream\":\"available\"}]}}}}}}";

    public static <T> T fromJson(String value, TypeReference<T> type) {
        T result = null;
        try {
            result = objectMapper.readValue(value, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static <T> T fromJson(String value, Class<T> type) {
        T result = null;
        try {
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            result = objectMapper.readValue(value, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public static <T> String toJson(T value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromJson(String value, JavaType type) {
        T result = null;
        try {
            result = (T) objectMapper.readValue(value, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static JavaType getCollectionType(Class<?> collectionClass, Class<?>... elementClasses) {
        return objectMapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
    }

    public static JsonNode getJsonNode(String value) {
        try {
            return objectMapper.readTree(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getNodeValue(String name, JsonNode node) {
        JsonNode nameNode = node.findValue(name);
        if (nameNode == null) {
            return "";
        }
        return nameNode.asText();
    }


}


