IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac.
PLATFORM ?= linux/amd64
DEV   ?= dev
ANSWER ?= dev-answer
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev local local-fixtures

## Build the image for the scorer's platform
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh

## Make the 1M-event dev set with the published generator (needs `generate` on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Compare the dev output with the published dev answer
check-dev:
	python3 scripts/compare.py out $(ANSWER)

## Build and test without Docker (needs a C++17 compiler)
local:
	$(CXX) -std=c++20 -O3 -march=native -pthread -o pipeline src/main.cpp

local-fixtures: local
	python3 scripts/run_fixtures.py fixtures -- ./pipeline
