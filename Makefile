CUBE_UDF_JARNAME = CubeUDF-1.0.jar

all: build
build:
	echo "$(MAVEN) clean package -DskipTests"
	set -o pipefail && $(MAVEN) clean package -DskipTests | tee -a build_cubeudf.log
	cp target/$(CUBE_UDF_JARNAME) $(TRAF_HOME)/export/lib
clean:
	$(RM) $(TRAF_HOME)/export/lib/$(CUBE_UDF_JARNAME)
	-$(MAVEN) -f pom.xml clean | grep ERROR
