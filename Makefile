
uberjar-build:
	lein uberjar

uberjar-run:
	java -jar ./target/usnpi.jar

migrate:
	lein migratus migrate

create-migration:
	@read -p "Enter migration name: " migration \
	&& lein migratus create $$migration
