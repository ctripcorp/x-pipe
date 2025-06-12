package com.ctrip.xpipe.redis.console;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.generator.AutoGenerator;
import com.baomidou.mybatisplus.generator.config.DataSourceConfig;
import com.baomidou.mybatisplus.generator.config.GlobalConfig;
import com.baomidou.mybatisplus.generator.config.OutputFile;
import com.baomidou.mybatisplus.generator.config.PackageConfig;
import com.baomidou.mybatisplus.generator.config.StrategyConfig;
import com.baomidou.mybatisplus.generator.config.TemplateConfig;
import com.baomidou.mybatisplus.generator.config.rules.DateType;
import com.baomidou.mybatisplus.generator.fill.Column;
import com.ctrip.xpipe.redis.console.entity.BaseEntity;
import org.junit.Test;

import java.util.Collections;

public class MybatisGeneratorTest {

    private static final DataSourceConfig DATA_SOURCE_CONFIG = new DataSourceConfig
        .Builder("jdbc:mysql://127.0.0.1:3306/fxxpipe?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai",
        "root", "123456")
        .build();

    /**
     * 全局配置
     */
    protected static GlobalConfig globalConfig() {
        return new GlobalConfig.Builder()
            .author("mybatis-generator")
            //            .enableSwagger()
            .outputDir("/Users/ccsa/prog/ctrip-framework/x-pipe/redis/redis-console/src/main/java")
            // 时间类型均生成Date对象
            .dateType(DateType.ONLY_DATE)
            .disableOpenDir()
            .build();
    }

    /**
     * 包配置
     */
    protected static PackageConfig packageConfig() {
        return new PackageConfig.Builder()
            .parent("com.ctrip.xpipe.redis.console")
            .pathInfo(Collections.singletonMap(OutputFile.xml, "/Users/ccsa/prog/ctrip-framework/x-pipe/redis/redis-console/src/main/resources/mapper"))
            .build();
    }

    /**
     * 策略配置
     */
    protected static StrategyConfig strategyConfig() {
        return new StrategyConfig.Builder()
            .addInclude("migration_bi_cluster_tbl")
            // 删除tbl后缀
            .addTableSuffix("_tbl")
            .entityBuilder()
            .superClass(BaseEntity.class)
            .enableFileOverride()
            .enableColumnConstant()
            .enableChainModel()
            //            .enableRemoveIsPrefix()
            .enableTableFieldAnnotation()
            .formatFileName("%sEntity")
            .logicDeleteColumnName("deleted")
            .addTableFills(
                new Column("create_time", FieldFill.INSERT),
                new Column("DataChange_LastTime", FieldFill.INSERT_UPDATE)
            )
            .build();
    }

    /**
     * 模板配置
     */
    protected static TemplateConfig templateConfig() {
        return new TemplateConfig.Builder()
            // 不生成controller service serviceImpl文件
            .controller("")
            .service("")
            .serviceImpl("")
            .build();
    }

    @Test
    public void generate() {
        AutoGenerator generator = new AutoGenerator(DATA_SOURCE_CONFIG);
        generator.global(globalConfig());
        generator.packageInfo(packageConfig());
        generator.strategy(strategyConfig());
        generator.template(templateConfig());
        generator.execute();
    }
}
