
.PHONY: deps compile rel tests

all: compile

REBAR=$(shell ./rebar --version > /dev/null 2>&1 && echo "./rebar" || echo "rebar")

deps:
	$(REBAR) get-deps

compile:
	$(REBAR) compile

ut:
	$(REBAR) eunit

ct: compile
	$(REBAR) ct

tests: ut ct
