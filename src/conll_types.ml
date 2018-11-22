open Printf

exception Error of Yojson.Basic.json

let error ?file ?line ?fct ?data msg =
  let opt_list = [
    Some ("message", `String msg);
    (CCOpt.map (fun x -> ("file", `String x)) file);
    (CCOpt.map (fun x -> ("line", `Int x)) line);
    Some ("library", `String "Conll");
    (CCOpt.map (fun x -> ("function", `String x)) fct);
    (CCOpt.map (fun x -> ("data", `String x)) data);
  ] in
  let json = `Assoc (CCList.filter_map (fun x->x) opt_list) in
  raise (Error json)

(* ======================================================================================================================== *)
module type Id_type = sig
  type t = int * int option (* 8.1 --> (8, Some 1) *)

  val to_string: t -> string

  val to_dot: t -> string

  val to_int: t -> int option

  exception Wrong_id of string

  val of_string: string -> t
  val of_int: int -> t

  val compare: t -> t -> int

  (* [min id1 id2] return the smallest id (according to compare) *)
  val min: t -> t -> t

  (* [max id1 id2] return the biggest id (according to compare) *)
  val max: t -> t -> t

  (* [min_max id1 id2] return (min,max) in a signle call *)
  val min_max: t -> t -> (t*t)

  (* [shift delta id] increases the position by delta *)
  val shift: int -> t -> t
end

(* ======================================================================================================================== *)
module Id = struct
  type t = int * int option (* 8.1 --> (8, Some 1) *)

  let to_string = function
    | (i, None) -> sprintf "%d" i
    | (i, Some j) -> sprintf "%d.%d" i j

  let to_dot = function
    | (i, None) -> sprintf "%d" i
    | (i, Some j) -> sprintf "%d_%d" i j

  let to_int = function
    | (i, None) -> Some i
    | (i, Some _) -> None

  exception Wrong_id of string
  let of_string s =
    try
      match Str.split (Str.regexp "\\.") s with
      | [i] -> (int_of_string i, None)
      | [i;j] -> (int_of_string i, Some (int_of_string j))
      | _ -> raise (Wrong_id s)
    with Failure _ -> raise (Wrong_id s)

  let of_int i = (i,None)

  let compare id1 id2 =
    match (id1, id2) with
    | ((i1, _), (i2, _)) when i1 <> i2 -> Pervasives.compare i1 i2
    (* all other cases: i1=i2 *)
    | ((_,None), (_,Some _)) -> -1
    | ((_,Some _), (_,None)) -> 1
    | ((_,Some sub_i1), (_,Some sub_i2)) -> Pervasives.compare sub_i1 sub_i2
    | ((_,None), (_,None)) -> 0

  let min id1 id2 = if compare id1 id2 < 0 then id1 else id2
  let max id1 id2 = if compare id1 id2 < 0 then id2 else id1
  let min_max id1 id2 = if compare id1 id2 < 0 then (id1, id2) else (id2, id1)

  let shift delta (i,j) = (i+delta,j)
end

module Id_set = Set.Make (Id)
module Id_map = Map.Make (Id)

module Int_map = Map.Make (struct type t = int let compare = Pervasives.compare end)

