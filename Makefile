
.PHONY: compile rel tests

all: compile

REBAR ?= $(shell which ./rebar 2>/dev/null || which rebar)

compile:
	$(REBAR) compile

ut:
	$(REBAR) skip_deps=true eunit

ct: compile
	$(REBAR) ct

tests: ut ct

