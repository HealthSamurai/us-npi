#
# for local usage only
#

all: uberjar-build uberjar-run

repl:
	source ./env_dev && lein repl

repl-test:
	source ./env_test && lein with-profile +test repl

uberjar-build:
	lein uberjar

uberjar-run:
	source ./env_dev && java -jar ./target/usnpi.jar

migrate:
	lein migratus migrate

.PHONY: test
test:
	source ./env_test && lein test

.PHONY: deploy
deploy:
	git commit --allow-empty -m "Trigger deployment"
	git push

create-migration:
	@read -p "Enter migration name: " migration \
	&& lein migratus create $$migration

docker-build:
	docker build -t usnpi:$(shell git rev-parse --short HEAD) .

docker-bash:
	docker run --rm -it usnpi:$(shell git rev-parse --short HEAD) /bin/bash

git-commit:
	@echo $(shell git rev-parse --short HEAD)


toc-install:
	npm install --save markdown-toc

toc-build:
	node_modules/.bin/markdown-toc -i README.md
