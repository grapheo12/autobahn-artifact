.PHONY: all
all:
	CC=clang CXX=clang++ cargo build --release --features benchmark