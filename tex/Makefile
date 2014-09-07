.PHONY: all clean

all: informe.pdf

informe.pdf: informe.tex ../src/mapReduce.hs
	pdflatex -interaction=nonstopmode -halt-on-error informe.tex && \
	pdflatex -interaction=nonstopmode -halt-on-error informe.tex

clean:
	rm -f informe.pdf *.aux *.log *.toc *.out *.pdf