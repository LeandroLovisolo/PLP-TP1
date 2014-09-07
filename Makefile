BUNDLE       = PLP-TP1.tar.gz
BUNDLE_DIR   = PLP-TP1
BUNDLE_FILES = src tex Makefile README.md enunciado.pdf

.PHONY: all clean bundle

all: informe.pdf
	make -C src all

informe.pdf:
	make -C tex all
	cp tex/informe.pdf .

bundle: clean informe.pdf
	mkdir $(BUNDLE_DIR)
	cp $(BUNDLE_FILES) $(BUNDLE_DIR) -r
	cp informe.pdf $(BUNDLE_DIR)
	tar zcf $(BUNDLE) $(BUNDLE_DIR)
	rm -rf $(BUNDLE_DIR)

clean:
	make -C src clean
	make -C tex clean
	rm -rf informe.pdf $(BUNDLE) $(BUNDLE_DIR)