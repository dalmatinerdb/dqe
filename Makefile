REBAR = $(shell pwd)/rebar3

.PHONY: deps rel package

all: compile

compile:
	$(REBAR) compile

clean:
	-rm -r .eunit
	$(REBAR) clean

qc: clean all
	$(REBAR) as eqc eqc

test: qc

bench: all
	-rm -r .eunit
	$(REBAR) -D BENCH skip_deps=true eunit

###
### Docs
###
docs:
	$(REBAR) edoc

##
## Developer targets
##

xref:
	$(REBAR) xref

##
## Dialyzer
##

dialyzer:
	$(REBAR) dialyzer
