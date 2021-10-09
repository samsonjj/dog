
clean:
	rm -rf deps 
	rm -f requirements.txt
	rm -f package.zip
	rm -f deps.zip

collect_deps:
	mkdir -p deps/python
	pipenv lock -r > requirements.txt
	pip install -r requirements.txt --no-deps -t deps/python

build_deps: collect_deps
	rm -f deps.zip
	cd deps; zip -r ../deps.zip *; cd ..

build: clean build_deps
	zip package.zip main.py