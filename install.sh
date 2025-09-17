#!/bin/sh


#currdir=$(cd $(dirname ${BASH_SOURCE[0]}); pwd )
currdir=$(cd `dirname $0`; pwd)

cd $currdir


for _Version in `ls others/redis/clients/jedis/`; do
	mvn install:install-file -Dfile=others/redis/clients/jedis/$_Version/jedis-$_Version.jar -DpomFile=others/redis/clients/jedis/$_Version/jedis-$_Version.pom
done

for _Version in `ls com/ctrip/framework/framework-bom/`; do
	mvn install:install-file -Dfile=com/ctrip/framework/framework-bom/$_Version/framework-bom-$_Version.pom -DpomFile=com/ctrip/framework/framework-bom/$_Version/framework-bom-$_Version.pom -Dpackaging=pom
done

for _Version in `ls com/ctrip/framework/framework-foundation/`; do
	mvn install:install-file -Dfile=com/ctrip/framework/framework-foundation/$_Version/framework-foundation-$_Version.jar -DpomFile=com/ctrip/framework/framework-foundation/$_Version/framework-foundation-$_Version.pom
done

#1.5.6无法安装，报错
for _Version in `ls com/ctrip/framework/framework-parent/ `; do
	mvn install:install-file -Dfile=com/ctrip/framework/framework-parent/$_Version/framework-parent-$_Version.pom -DpomFile=com/ctrip/framework/framework-parent/$_Version/framework-parent-$_Version.pom -Dpackaging=pom
done

for _Version in `ls com/ctrip/super-pom/`; do
	mvn install:install-file -Dfile=com/ctrip/super-pom/$_Version/super-pom-$_Version.pom -DpomFile=com/ctrip/super-pom/$_Version/super-pom-$_Version.pom -Dpackaging=pom
done

for _Version in `ls ctripgroup/scm/super-rule/ | grep -v maven-metadata`; do
	mvn install:install-file -Dfile=ctripgroup/scm/super-rule/$_Version/super-rule-$_Version.pom -DpomFile=ctripgroup/scm/super-rule/$_Version/super-rule-$_Version.pom -Dpackaging=pom
done

for _Version in `ls com/ctrip/ctrip-super-rule/ | grep -v maven-metadata | grep -v resolver-status`; do
	mvn install:install-file -Dfile=com/ctrip/ctrip-super-rule/$_Version/ctrip-super-rule-$_Version.pom -DpomFile=com/ctrip/ctrip-super-rule/$_Version/ctrip-super-rule-$_Version.pom -Dpackaging=pom
done

for _Version in `ls com/ctrip/thirdparty/ctrip-thirdparty-bom/`; do
	mvn install:install-file -Dfile=com/ctrip/thirdparty/ctrip-thirdparty-bom/$_Version/ctrip-thirdparty-bom-$_Version.pom -DpomFile=com/ctrip/thirdparty/ctrip-thirdparty-bom/$_Version/ctrip-thirdparty-bom-$_Version.pom -Dpackaging=pom
done

for _Version in `ls io/grpc/grpc-bom/`; do
	mvn install:install-file -Dfile=io/grpc/grpc-bom/$_Version/grpc-bom-$_Version.pom -DpomFile=io/grpc/grpc-bom/$_Version/grpc-bom-$_Version.pom -Dpackaging=pom
done

for _Version in `ls com/ctrip/arch/arch-bom/`; do
	mvn install:install-file -Dfile=com/ctrip/arch/arch-bom/$_Version/arch-bom-$_Version.pom -DpomFile=com/ctrip/arch/arch-bom/$_Version/arch-bom-$_Version.pom -Dpackaging=pom
done

for _Version in `ls qunar/tc/tcdev/`; do
	mvn install:install-file -Dfile=qunar/tc/tcdev/$_Version/tcdev-$_Version.pom -DpomFile=qunar/tc/tcdev/$_Version/tcdev-$_Version.pom -Dpackaging=pom
done

for _Version in `ls qunar/tc/thirdparty-dependencies/`; do
	mvn install:install-file -Dfile=qunar/tc/thirdparty-dependencies/$_Version/thirdparty-dependencies-$_Version.pom -DpomFile=qunar/tc/thirdparty-dependencies/$_Version/thirdparty-dependencies-$_Version.pom -Dpackaging=pom
done

for _Version in `ls qunar/common/qunar-suprule/`; do
	mvn install:install-file -Dfile=qunar/common/qunar-suprule/$_Version/qunar-suprule-$_Version.pom -DpomFile=qunar/common/qunar-suprule/$_Version/qunar-suprule-$_Version.pom -Dpackaging=pom
done

for _Version in `ls qunar/common/qunar-supom/`; do
	mvn install:install-file -Dfile=qunar/common/qunar-supom/$_Version/qunar-supom-$_Version.pom -DpomFile=qunar/common/qunar-supom/$_Version/qunar-supom-$_Version.pom -Dpackaging=pom
done

for _Version in `ls org/springframework/boot/spring-boot-dependencies/`; do
	mvn install:install-file -Dfile=org/springframework/boot/spring-boot-dependencies/$_Version/spring-boot-dependencies-$_Version.pom -DpomFile=org/springframework/boot/spring-boot-dependencies/$_Version/spring-boot-dependencies-$_Version.pom -Dpackaging=pom
done

for _Version in `ls com/dianping/cat/cat-exporter/`; do
	mvn install:install-file -Dfile=com/dianping/cat/cat-exporter/$_Version/cat-exporter-$_Version.jar -DpomFile=com/dianping/cat/cat-exporter/$_Version/cat-exporter-$_Version.pom
done

for _Version in `ls com/dianping/cat/cat-client/`; do
	mvn install:install-file -Dfile=com/dianping/cat/cat-client/$_Version/cat-client-$_Version.jar -DpomFile=com/dianping/cat/cat-client/$_Version/cat-client-$_Version.pom
done

for _Version in `ls com/dianping/cat/parent/`; do
	mvn install:install-file -Dfile=com/dianping/cat/parent/$_Version/parent-$_Version.pom -DpomFile=com/dianping/cat/parent/$_Version/parent-$_Version.pom -Dpackaging=pom
done



for _Version in `ls org/codehaus/plexus/plexus-container-default/`; do
	mvn install:install-file -Dfile=org/codehaus/plexus/plexus-container-default/$_Version/plexus-container-default-$_Version.jar -DpomFile=org/codehaus/plexus/plexus-container-default/$_Version/plexus-container-default-$_Version.pom
done



for _Version in `ls org/unidal/framework/dal-jdbc/`; do
	mvn install:install-file -Dfile=org/unidal/framework/dal-jdbc/$_Version/dal-jdbc-$_Version.jar -DpomFile=org/unidal/framework/dal-jdbc/$_Version/dal-jdbc-$_Version.pom
done



for _Version in `ls org/unidal/framework/foundation-service`; do
	mvn install:install-file -Dfile=org/unidal/framework/foundation-service/$_Version/foundation-service-$_Version.jar -DpomFile=org/unidal/framework/foundation-service/$_Version/foundation-service-$_Version.pom
done



for _Version in `ls org/unidal/framework/parent/ | grep -v maven-metadata | grep -v resolver-status`; do
	mvn install:install-file -Dfile=org/unidal/framework/parent/$_Version/parent-$_Version.pom -DpomFile=org/unidal/framework/parent/$_Version/parent-$_Version.pom -Dpackaging=pom
done

for _Version in `ls org/unidal/maven/plugins/codegen`; do
	mvn install:install-file -Dfile=org/unidal/maven/plugins/codegen/$_Version/codegen-$_Version.jar -DpomFile=org/unidal/maven/plugins/codegen/$_Version/codegen-$_Version.pom
done


for _Version in `ls org/unidal/maven/plugins/codegen-maven-plugin`; do
	mvn install:install-file -Dfile=org/unidal/maven/plugins/codegen-maven-plugin/$_Version/codegen-maven-plugin-$_Version.jar -DpomFile=org/unidal/maven/plugins/codegen-maven-plugin/$_Version/codegen-maven-plugin-$_Version.pom
done



for _Version in `ls org/unidal/maven/plugins/common/`; do
	mvn install:install-file -Dfile=org/unidal/maven/plugins/common/$_Version/common-$_Version.jar -DpomFile=org/unidal/maven/plugins/common/$_Version/common-$_Version.pom
done



for _Version in `ls org/unidal/maven/plugins/default/`; do
	mvn install:install-file -Dfile=org/unidal/maven/plugins/default/$_Version/default-$_Version.pom -DpomFile=org/unidal/maven/plugins/default/$_Version/default-$_Version.pom -Dpackaging=pom
done


for _Version in `ls org/unidal/maven/plugins/plexus-maven-plugin`; do
	mvn install:install-file -Dfile=org/unidal/maven/plugins/plexus-maven-plugin/$_Version/plexus-maven-plugin-$_Version.jar -DpomFile=org/unidal/maven/plugins/plexus-maven-plugin/$_Version/plexus-maven-plugin-$_Version.pom
done

for _Version in `ls org/unidal/framework/test-framework`; do
	mvn install:install-file -Dfile=org/unidal/framework/test-framework/$_Version/test-framework-$_Version.jar -DpomFile=org/unidal/framework/test-framework/$_Version/test-framework-$_Version.pom
done


