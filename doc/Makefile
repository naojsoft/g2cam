#
# Makefile for g2cam manual
#
# E. Jeschke  2012.01.25
#

DOC	= g2cam

all:	pdf

pdf:	$(DOC).pdf
ps:	$(DOC).ps
dvi:	$(DOC).dvi

# $(DOC).pdf:	$(DOC).ps
# 	ps2pdf $(DOC).ps $(DOC).pdf 

# Run xelatex twice so that TOC and figure references get written correctly
$(DOC).pdf:	$(DOC).tex
	#pdflatex $(DOC).tex
	xelatex $(DOC).tex
	xelatex $(DOC).tex

$(DOC).ps:	$(DOC).dvi
	dvi2ps -c $(DOC).ps $(DOC).dvi 

$(DOC).dvi:	$(DOC).tex
	latex $(DOC)
	bibtex $(DOC)
	latex $(DOC)
	latex $(DOC)

clean:
	/bin/rm -f *.aux *.log *.pdf *.toc

