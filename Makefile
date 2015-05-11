
.PHONY: compile rel ct doc

all: compile

REBAR ?= $(shell which ./rebar 2>/dev/null || which rebar)

PLT_APPS = kernel stdlib erts eunit
PLT = gululog_dialyzer.plt

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

check_plt:
	dialyzer --check_plt --plt $(PLT) --apps $(APPS)

build_plt:
	dialyzer --build_plt --output_plt $(PLT) --apps $(PLT_APPS)

dyz:
	dialyzer -Wno_return -Wunmatched_returns -Wrace_conditions --plt $(PLT) ebin | tee .dialyzer.raw-output

