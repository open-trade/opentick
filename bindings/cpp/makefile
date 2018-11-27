release:
	mkdir -p build/release; cd build/release; cmake ../.. -DCMAKE_BUILD_TYPE=Release; make ${args}; cd -;

debug:
	mkdir -p build/debug; cd build/debug; cmake ../.. -DCMAKE_BUILD_TYPE=Debug; make ${args}; cd -;

clean:
	rm -rf build;
