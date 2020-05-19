OCB_FLAGS = -use-ocamlfind -I src
OCB = ocamlbuild $(OCB_FLAGS)

LIB_FILES = conll.cma conll.cmxa conll.a conll.cmi conll.cmx conll.cmxs conll_types.cmi conll_types.cmx \
	conllx.cma conllx.cmxa conllx.a conllx.cmi conllx.cmx conllx.cmxs
INSTALL_FILES = $(LIB_FILES:%=_build/src/%)

VERSION = `cat VERSION`

build:
	$(OCB) $(LIB_FILES)

install: build uninstall
	ocamlfind install -patch-version $(VERSION) conll META $(INSTALL_FILES)

uninstall:
	ocamlfind remove conll

tool:
	ocamlbuild -use-ocamlfind -pkg yojson  -pkg conll -I src_tool conll_tool.native

install_tool: tool
	cp conll_tool.native ~/.local/bin/conll_tool

.PHONY: all clean build

clean:
	$(OCB) -clean
	rm -f conll_tool.native
