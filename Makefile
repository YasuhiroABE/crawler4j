
.PHONY: all
all:
	@echo "[usage] make [c|b|p]"
	@echo "   c: execute ./gradlew compileJava"
	@echo "   b: execute ./gradlew clean build"
	@echo "   p: execute . ./envrc && ./gradlew sonatypeCentralUpload"

.PHONY: c
c:
	@echo Compile code
	./gradlew compileJava

.PHONY: b
b:
	@echo Build jar
	./gradlew clean build

.PHONY: p
p:
	@echo Publish jar to maven central
	( . ./envrc && ./gradlew sonatypeCentralUpload )

