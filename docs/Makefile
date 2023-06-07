# Makefile for Sphinx documentation
# Borrowing ideas from Spack's documentation generator.

# You can set these variables from the command line.
SPHINXBUILD   = sphinx-build
BUILDDIR      = _build

all: html

# Build HTML
html:
	$(SPHINXBUILD) -b html . $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

# This creates a git repository and commits generated html docs.
# It them pushes the new branch into THIS repository as gh-pages.
#
# github for some reason runs jekyll automatically on gh-pages
# files, but we don't want that.  'touch .nojekyll' takes care
# of it.
gh-pages: _build/html
	root="$$(git rev-parse --show-toplevel)" && \
	cd _build/html && \
	rm -rf .git && \
	touch .nojekyll && \
	git init && \
	git add . && \
	git commit -m "DSI Documentation" && \
	git push -f $$root master:gh-pages && \
	rm -rf .git

# Push to external docs site
publish:
	git push -f origin gh-pages

# Print usage
help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  html       to make standalone HTML files"
	@echo "  gh-pages   move documentation to root of gh-pages branch"
	@echo "  publish    upload documentation to github"

clean:
	-rm -rf $(BUILDDIR)/* 