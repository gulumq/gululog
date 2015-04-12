
.PHONY: deps compile rel

all: compile

REBAR=$(shell which rebar > /dev/null 2>&1 && echo "rebar" || "./rebar")

deps:
	$(REBAR) get-deps

compile:
	$(REBAR) compile

ct: compile
	$(REBAR) ct


