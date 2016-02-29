OCB_FLAGS = -use-ocamlfind -I src
OCB = ocamlbuild $(OCB_FLAGS)

LIB_FILES = conll.cma conll.cmxa conll.a conll.cmi
INSTALL_FILES = $(LIB_FILES:%=_build/src/%)

VERSION = `cat VERSION`

build:
	$(OCB) $(LIB_FILES)

install: build uninstall
	ocamlfind install -patch-version $(VERSION) conll META $(INSTALL_FILES)

uninstall:
	ocamlfind remove conll

.PHONY: all clean build

clean:
	$(OCB) -clean
