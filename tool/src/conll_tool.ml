open Printf
open Conll


let dump_id_sentence corpus =
	Array.iter
		(fun (id,conll) ->
			let sentence = match Conll.get_sentence conll with
			| Some s -> s
			| None -> Conll.build_sentence conll in
			printf "%s#%s\n" id sentence
		) corpus

let sentid corpus =
	let new_corpus = Array.map
		(fun (id, conll) -> (id, Conll.ensure_sentid_in_meta conll)
		) corpus in
	Conll_corpus.dump new_corpus

let fusion corpus =
	let new_corpus = Array.map
		(fun (id, conll) -> (id, Conll.normalize_multiwords conll)
		) corpus in
	Conll_corpus.dump new_corpus



let print_usage () =
	List.iter (fun x -> printf "%s\n" x)
	[
	"Usage:";
	" * conll_tool.native sentences <file>       dump on stdout the list of \"id#sentence\"";
	" * conll_tool.native sentid <file>          dump the input corpus with #sentid moved into metadata when necessary";
	]

let _ =
	match List.tl (Array.to_list Sys.argv) with

	| ["fusion"; corpus_name] ->
		let corpus = Conll_corpus.load corpus_name in
		fusion corpus
	| "fusion"::_ -> printf "ERROR: sub-command \"fusion\" expects one argument\n"; print_usage ()

	| ["sentences"; corpus_name] ->
		let corpus = Conll_corpus.load corpus_name in
		dump_id_sentence corpus
	| "sentences"::_ -> printf "ERROR: sub-command \"sentences\" expects one argument\n"; print_usage ()

	| ["sentid"; corpus_name] ->
		let corpus = Conll_corpus.load corpus_name in
		sentid corpus
	| "sentid"::_ -> printf "ERROR: sub-command \"sentid\" expects one argument\n"; print_usage ()

	| ["dump"; corpus_name] ->
		let corpus = Conll_corpus.load corpus_name in
		Conll.dump (snd corpus.(0))
	| "dump"::_ -> printf "ERROR: sub-command \"dump\" expects one argument\n"; print_usage ()

	| [] -> print_usage ()
	| x :: _ ->
		printf "ERROR: unknown sub-command \"%s\"\n" x; print_usage ()