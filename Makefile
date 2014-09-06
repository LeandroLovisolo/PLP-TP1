.PHONY: all clean

all:
	make -C src all
	make -C tex all
	cp tex/informe.pdf .

clean:
	make -C src clean
	make -C tex clean
	rm -f informe.pdf