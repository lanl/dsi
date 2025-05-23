# Makefile for Sphinx documentation
# Borrowing ideas from Spack's documentation generator.

# You can set these variables from the command line.
SPHINXBUILD   = sphinx-build
BUILDDIR      = _build

all: html

# Build HTML
html:
	python ./plugins/generate_plugin_class_hierarchy.py ../dsi/plugins/
	python ./backends/generate_backend_class_hierarchy.py ../dsi/backends/
	mv PluginClassHierarchy.gv.png images/PluginClassHierarchy.png
	mv BackendClassHierarchy.gv.png images/BackendClassHierarchy.png
	rm PluginClassHierarchy.gv
	rm BackendClassHierarchy.gv
	rm -rf $(BUILDDIR)/html
	$(SPHINXBUILD) -b html . $(BUILDDIR)/html
#	pytest --cov --cov-report=html ../
#	mv ./htmlcov $(BUILDDIR)/html
	rm -f ../coverage.svg
#	coverage-badge -o ../coverage.svg
#	git add ../coverage.svg
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

# This creates a git repository and commits generated html docs.
# It them pushes the new branch into THIS repository as gh-pages.
#
# github for some reason runs jekyll automatically on gh-pages
# files, but we don't want that.  'touch .nojekyll' takes care
# of it.
#	git add --force ./htmlcov && \

gh-pages: _build/html
	cd _build/html && \
	rm -rf .git && \
	touch .nojekyll && \
	git init && \
	git add . && \
	git commit -m "DSI Documentation" && \
	git branch -M gh-pages && \
	git remote add origin https://github.com/lanl/dsi.git && \
	git push -f origin gh-pages && \
	rm -rf .git

# # Push to external docs site
# publish:
# 	git push -f origin gh-pages

# Make a PDF
latexpdf:
	$(SPHINXBUILD) -b latex . $(BUILDDIR)/latex
	@echo "Running LaTeX files through pdflatex..."
	$(MAKE) -C $(BUILDDIR)/latex all-pdf
	@echo "pdflatex finished; the PDF files are in $(BUILDDIR)/latex."

# Print usage
help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  html       to make standalone HTML files"
	@echo "  gh-pages   move documentation to root of gh-pages branch"
	@echo "  publish    upload documentation to github"

clean:
	-rm -rf $(BUILDDIR)/* *ClassHierarchy.gv *Hierarchy.png
