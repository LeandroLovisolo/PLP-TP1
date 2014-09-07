BUNDLE = PLP-TP1.tar.gz
BUNDLE_DIR = bundle
BUNDLE_FILES = src tex Makefile README.md enunciado.pdf

.PHONY: all clean bundle

all:
	make -C src all
	make -C tex all
	cp tex/informe.pdf .

bundle: clean
	mkdir $(BUNDLE_DIR)
	cp $(BUNDLE_FILES) $(BUNDLE_DIR) -r
	make
	cp informe.pdf $(BUNDLE_DIR)
	tar zcf $(BUNDLE) $(BUNDLE_DIR)
	rm -rf $(BUNDLE_DIR)

clean:
	make -C src clean
	make -C tex clean
	rm -f informe.pdf