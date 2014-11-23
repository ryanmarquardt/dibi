
PYTHON=python3
SETUP=$(PYTHON) setup.py

VERSION=$(shell $(SETUP) --version)
FULLNAME=$(shell $(SETUP) --fullname)
NAME=$(shell $(SETUP) --name)

SRCFILES=$(shell find $(NAME) -name '*.py')

TESTPYTHON=PYTHONPATH=$(PWD)/build/lib $(PYTHON)

.PHONY: test all sdist

all: build

test: build
	find $(NAME) -name '*.py' -exec pep8 --show-source '{}' \;
	find test -name '*.py' -exec pep8 --show-source '{}' \;
	$(TESTPYTHON) -m test

build: $(SRCFILES)
	@$(SETUP) build
	@touch build

sdist: $(FULLNAME).tar.gz

$(FULLNAME).tar.gz:
	@$(SETUP) sdist
