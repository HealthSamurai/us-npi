#
# for local usage only
#

all: uberjar-build uberjar-run

repl:
	lein repl

uberjar-build:
	lein uberjar

uberjar-run:
	java -jar ./target/usnpi.jar

migrate:
	lein migratus migrate

.PHONY: test
test:
	lein test

.PHONY: deploy
deploy:
	git commit --allow-empty -m "Trigger deployment"
	git push

create-migration:
	@read -p "Enter migration name: " migration \
	&& lein migratus create $$migration
