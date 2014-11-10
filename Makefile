VERSION=$(shell python setup.py --version)
FULLNAME=$(shell python setup.py --fullname)
NAME=$(shell python setup.py --name)

SRCFILES=$(shell find $(NAME) -name '*.py')

TESTPYTHON=PYTHONPATH=$(PWD)/build/lib python
SETUP=python3 setup.py

.PHONY: test all sdist

all: build

test: build
	find $(NAME) -name '*.py' -exec pep8 --show-source '{}' \;
	$(TESTPYTHON)3 dibi/__init__.py test
	$(TESTPYTHON)2 dibi/__init__.py test

build: $(SRCFILES)
	@$(SETUP) build
	@touch build

sdist: $(FULLNAME).tar.gz

$(FULLNAME).tar.gz:
	@$(SETUP) sdist
