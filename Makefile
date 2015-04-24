
.PHONY: compile rel tests

all: compile

REBAR ?= $(shell which ./rebar 2>/dev/null || which rebar)

compile:
	$(REBAR) compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean

tests: REBAR := GULULOG_TEST=1 $(REBAR)
tests: deps compile
	$(REBAR) skip_deps=true eunit
	$(REBAR) ct

