CREATE TABLE "Timestamps" (
  "id" SERIAL PRIMARY KEY,
  "commit" text,
  "timestamp" timestamptz,
  "host" text
);

CREATE TABLE "Measurements" (
  "id" SERIAL PRIMARY KEY,
  "timepoint" int,
  "experiment" int,
  "benchmark" int,
  "suite" int,
  "config" int,
  "case" text,
  "value" float
);

CREATE TABLE "Experiments" (
  "id" SERIAL,
  "benchmark" int,
  "suite" int,
  "name" text,
  "version" int,
  "description" text,
  "is_read_only" bool,
  "label" text,
  PRIMARY KEY ("id", "benchmark", "suite")
);

CREATE TABLE "Configurations" (
  "id" SERIAL PRIMARY KEY,
  "name" text,
  "parameters" text
);

CREATE TABLE "Benchmarks" (
  "id" SERIAL,
  "suite" int,
  "name" text,
  PRIMARY KEY ("id", "suite")
);

CREATE TABLE "Suites" (
  "id" SERIAL PRIMARY KEY,
  "name" text
);

ALTER TABLE "Measurements" ADD FOREIGN KEY ("timepoint") REFERENCES "Timestamps" ("id");

ALTER TABLE "Measurements" ADD FOREIGN KEY ("config") REFERENCES "Configurations" ("id");

ALTER TABLE "Measurements" ADD FOREIGN KEY ("experiment", "benchmark", "suite") REFERENCES "Experiments" ("id", "benchmark", "suite");

ALTER TABLE "Experiments" ADD FOREIGN KEY ("benchmark", "suite") REFERENCES "Benchmarks" ("id", "suite");

ALTER TABLE "Benchmarks" ADD FOREIGN KEY ("suite") REFERENCES "Suites" ("id");

CREATE UNIQUE INDEX ON "Timestamps" ("timestamp", "host");

CREATE UNIQUE INDEX ON "Experiments" ("name", "version");

CREATE UNIQUE INDEX ON "Configurations" ("name", "parameters");

CREATE UNIQUE INDEX ON "Benchmarks" ("suite", "name");

CREATE UNIQUE INDEX ON "Suites" ("name");
