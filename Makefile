
.PHONY: compile rel ct doc

all: compile

REBAR ?= $(shell which ./rebar 2>/dev/null || which rebar)

compile:
	$(REBAR) compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean

doc:
	$(REBAR) doc

xref: deps compile
	$(REBAR) xref

ct: REBAR := TEST=1 $(REBAR)
ct: deps compile
	$(REBAR) skip_deps=true ct

