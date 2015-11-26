REBAR = rebar3

.PHONY: all test tree

all: compile

include fifo.mk

clean:
	$(REBAR) clean
