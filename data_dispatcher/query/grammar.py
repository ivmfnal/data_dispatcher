ProjectQueryGrammar = """
?metadata_expression:   meta_or                                                           

meta_or:    meta_and ( "or" meta_and )*

meta_and:   term_meta ( "and" term_meta )*

?term_meta:  scalar CMPOP constant                  -> cmp_op
    | scalar "in" constant ":" constant             -> in_range
    | scalar "not" "in" constant ":" constant       -> not_in_range
    | scalar "in" "(" constant_list ")"             -> in_set
    | scalar "not" "in" "(" constant_list ")"       -> not_in_set
    | ANAME "present"?                              -> present                   
    | ANAME "not" "present"                         -> not_present                   
    | constant "in" ANAME                           -> constant_in_array
    | constant "not" "in" ANAME                     -> constant_not_in_array
    | "(" metadata_expression ")"                              
    | "!" term_meta                                 -> meta_not

scalar:   attribute
        | ANAME
        | ANAME "[" "all" "]"                       -> array_all
        | ANAME "[" "any" "]"                       -> array_any
        | ANAME "[" SIGNED_INT "]"                  -> array_subscript
        | ANAME "[" STRING "]"                      -> array_subscript
        | "len" "(" ANAME ")"                       -> array_length

!attribute: ("_owner" | "_state" | "_created" | "_ended" | "_id" | "_query")

constant_list:    constant ("," constant)*                    

constant : SIGNED_FLOAT                             -> float_constant                      
    | STRING                                        -> string_constant
    | SIGNED_INT                                    -> int_constant
    | BOOL                                          -> bool_constant

index:  STRING
    | SIGNED_INT

ANAME: WORD ("." WORD)*

FNAME: LETTER ("_"|"-"|"."|LETTER|DIGIT|"/")*

FID: ("_"|"-"|"."|LETTER|DIGIT|"/")+

WORD: LETTER ("_"|LETTER|DIGIT)*

CMPOP:  "<" "="? | "!"? "=" "="? | "!"? "~" "*"? | ">" "="? | "like"            //# like is not implemented yet

BOOL: "true"i | "false"i

STRING : /("(?!"").*?(?<!\\\\)(\\\\\\\\)*?"|'(?!'').*?(?<!\\\\)(\\\\\\\\)*?')/i


%import common.CNAME
%import common.SIGNED_INT
%import common.SIGNED_FLOAT

%import common.WS
%import common.LETTER
%import common.DIGIT
%ignore WS
"""
