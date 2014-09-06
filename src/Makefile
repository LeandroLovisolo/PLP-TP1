.PHONY: all clean

all: tests

tests: mapReduce.hs mapReduceSpec.hs
	ghc -o tests mapReduce.hs mapReduceSpec.hs

clean:
	rm -f tests *.hi *.o