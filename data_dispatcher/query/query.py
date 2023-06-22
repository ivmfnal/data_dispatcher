from lark import Lark
from .grammar import ProjectQueryGrammar
from metacat.common.trees import Converter, Node
from metacat.common import MetaExpressionDNF
from metacat.util import insert_sql
from data_dispatcher.db import DBProject

class ProjectQueryConverter(Converter):
    
    def __default__(self, typ, children, meta):
        return Node(typ, children, _meta=meta)

    def int_constant(self, args):
        v = args[0]
        return Node("int", value=int(v.value))
        
    def float_constant(self, args):
        v = args[0]
        return Node("float", value=float(v.value))

    def bool_constant(self, args):
        v = args[0]
        #print("bool_constant:", args, args[0].value)
        return Node("bool", value=v.value.lower() == "true")

    def string_constant(self, args):
        v = args[0]
        s = v.value
        if s[0] in ('"', "'"):
            s = s[1:-1]
        if '"' in s or "'" in s:        # sanitize
            raise ValueError("Unsafe string constant containing double or single quote: %s" % (repr(s),))
        return Node("string", value=s)

    def constant_list(self, args):
        return [n["value"] for n in args]

    def scalar(self, args):
        (t,) = args
        if isinstance(t, Node) and t.T == "attribute":
            return t
        return Node("scalar", name=t.value)

    def array_any(self, args):
        (n,) = args
        return Node("array_any", name=n.value)
        
    def array_all(self, args):
        (n,) = args
        return Node("array_all", name=n.value)
        
    def array_length(self, args):
        (n,) = args
        return Node("array_length", name=n.value)
        
    def array_subscript(self, args):
        name, inx = args
        if inx.type == "STRING":
            inx = inx.value[1:-1]
        else:
            inx = int(inx.value)
        return Node("array_subscript", name=name.value, index=inx)

    def cmp_op(self, args):
        node = Node("cmp_op", [args[0], args[2]], op=args[1].value, neg=False)
        return self._convert_array_all(node)
        
    def constant_in_array(self, args):
        return Node("cmp_op",
            [Node("array_any", name=args[1].value), args[0]], op="=", neg=False
        )
        
    def constant_not_in_array(self, args):
        return Node("cmp_op",
            [Node("array_any", name=args[1].value), args[0]], op="=", neg=True
        )

    def constant_in_attr(self, args):
        const_arg = args[0]
        const_type = const_arg.T
        const_value = const_arg["value"]
        assert const_type == "string"
        return Node("cmp_op", 
                    [
                        Node("object_attribute", name=args[1].value), 
                        Node("string", value=".*%s.*" % (const_value,))
                    ], op="~", neg=False
        )


    def constant_not_in_attr(self, args):
        out = self.constant_in_attr(args)
        return out.clone(neg = not out["neg"])

    def constant_in_meta(self, args):
        const_arg = args[0]
        const_type = const_arg.T
        const_value = const_arg["value"]
        array_in = Node("cmp_op", [Node("array_any", name=args[1].value), const_arg], op="=", neg=False)
        if const_type == "string":
            return Node("meta_or",
                [   array_in,
                    Node("cmp_op", [
                        Node("meta_attribute", name=args[1].value), 
                        Node("string", value=".*%s.*" % (const_value,))
                    ], op="~", neg=False)
                ]
            )
        else:
            return array_in
        
    def constant_not_in_meta(self, args):
        const_arg = args[0]
        const_type = const_arg.T
        const_value = const_arg["value"]
        array_not_in = Node("cmp_op", [Node("array_any", name=args[1].value), const_arg], op="=", neg=True)
        if const_type == "string":
            return Node("meta_and",
                [   array_not_in,
                    Node("cmp_op", [
                        Node("meta_attribute", name=args[1].value), 
                        Node("string", value=".*%s.*" % (const_value,))
                    ], op="~", neg=True)
                ]
            )
        else:
            return array_not_in

    def in_range(self, args):
        assert len(args) == 3 and args[1].T in ("string", "int", "float") and args[2].T in ("string", "int", "float")
        assert args[1].T == args[2].T, "Range ends must be of the same type"
        return self._convert_array_all(Node("in_range", [args[0]], low=args[1]["value"], high=args[2]["value"], neg=False, type=args[1].T))
    
    def not_in_range(self, args):
        assert len(args) == 3 and args[1].T in ("string", "int", "float") and args[2].T in ("string", "int", "float")
        assert args[1].T == args[2].T, "Range ends must be of the same type"
        return self._convert_array_all(Node("in_range", [args[0]], low=args[1]["value"], high=args[2]["value"], neg=True, type=args[1].T))

    def in_set(self, args):
        assert len(args) == 2
        return self._convert_array_all(Node("in_set", [args[0]], neg=False, set=args[1]))
        
    def not_in_set(self, args):
        assert len(args) == 2
        return self._convert_array_all(Node("in_set", [args[0]], neg=True, set=args[1]))
        
    def index(self, args):
        return args[0].value
        
    def meta_and(self, args):
        children = []
        for a in args:
            if a.T == "meta_and":
                children += a.C
            else:
                children.append(a)
        return Node("meta_and", children)
        
    def meta_or(self, args):
        children = []
        for a in args:
            if a.T == "meta_or":
                children += a.C
            else:
                children.append(a)
        return Node("meta_or", children)
        
    def present(self, args):
        assert len(args) == 1
        return Node("present", name = args[0].value)

    def not_present(self, args):
        assert len(args) == 1
        return Node("not_present", name = args[0].value)

    def _apply_not(self, node):
        
        def reverse_array_wildcard(node):
            if node.T == "array_any":
                node = node.clone()
                node.T = "array_all"
            elif node.T == "array_all":
                node = node.clone()
                node.T = "node_any"
            else:
                pass
            return node
        
        if node.T in ("meta_and", "meta_or") and len(node.C) == 1:
            return self._apply_not(node.C[0])
        if node.T == "meta_and":
            return Node("meta_or", [self._apply_not(c) for c in node.C])
        elif node.T == "meta_or":
            return Node("meta_and", [self._apply_not(c) for c in node.C])
        elif node.T == "meta_not":
            return node.C[0]
        elif node.T in ("cmp_op", "in_set", "in_range"):
            node["neg"] = not node["neg"]
            return node
        elif node.T == "cmp_op":
            new_op = {
                "~":   "!~",
                "!~":  "~",
                "~*":   "!~*",
                "!~*":  "~*",
                ">":    "<=",
                "<":    ">=",
                ">=":    "<",
                "<=":    ">",
                "=":    "!=",
                "==":    "!=",
                "!=":    "=="
            }[node["op"]]
            return node.clone(op=new_op)
        elif node.T == "present":
            return Node("not_present", name=node["name"])
        elif node.T == "not_present":
            return Node("present", name=node["name"])
        else:
            raise ValueError("Unknown node type %s while trying to apply NOT operation" % (node.T,))
            
    def meta_not(self, children):
        assert len(children) == 1
        return self._apply_not(children[0])
        
    def meta_attribute(self, args):
        assert len(args) == 1
        word = args[0].value
        return Node("meta_attribute", name=word)

    def object_attribute(self, args):
        assert len(args) == 1
        word = args[0].value
        assert word in ("owner", "state", "created", "ended", "id", "query")
        return Node("object_attribute", name=word)

    def _convert_array_all(self, node):
        left = node.C[0]
        if left.T == "array_all":
            if node.T == "cmp_op":
                new_op = {
                    "~":   "!~",
                    "!~":  "~",
                    "~*":   "!~*",
                    "!~*":  "~*",
                    ">":    "<=",
                    "<":    ">=",
                    ">=":    "<",
                    "<=":    ">",
                    "=":    "!=",
                    "==":    "!=",
                    "!=":    "=="
                }[node["op"]]
                node["op"] = new_op
            else:
                node.T = {
                    "in_set":"not_in_set",
                    "in_range":"not_in_range",
                    "not_in_set":"in_set",
                    "not_in_range":"in_range",
                }[node.T]
            left.T = "array_any"
            node["neg"] = not node["neg"]
        #print("_convert_array_all: returning:", node.pretty())
        return node

class ProjectQuery(object):
    
    QueryParser = Lark(ProjectQueryGrammar, start="metadata_expression")

    def __init__(self, text):
        self.Text = text
        self.Parsed = self.Converted = None
        
    def parse(self):
        self.Parsed = self.QueryParser.parse(self.Text)
        return self.Parsed
        
    def convert(self):
        self.Converted = ProjectQueryConverter()(self.parse())
        #print("converted:", self.Converted.pretty())
        return self.Converted
        
    def sql(self):
        table = DBProject.Table
        columns = DBProject.columns(table)
        meta_sql = MetaExpressionDNF(self.convert()).sql(table, "attributes")
        return insert_sql(f"""
            select {columns} 
                from {table} --
                where 
                    $meta_sql
        """, meta_sql=meta_sql)
