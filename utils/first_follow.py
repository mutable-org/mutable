#!env python3

from __future__ import annotations
from functools import reduce
from sortedcontainers import SortedSet, SortedDict
from typeguard import typechecked
from typing import TypeAlias
from typing_extensions import override
import argparse
import copy
import io
import os.path
import re
import subprocess
import sys


class DEFAULTS:
    BUILD_DIR = 'build/debug_shared'
    LEX_BINARY = 'bin/lex'


# Yields all the lines of \p file that are within code blocks (```).
def code_segments(file: io.TextIOBase):
    in_block = False
    for line in file:
        if line.startswith('```'):
            in_block = not in_block
        elif in_block:
            yield line
    return


# Yields all tokens within a sequence of lines.
def tokens(lines):
    for line in lines:
        for tok in line.split():
            yield tok
    return


# Representation of a language grammar.
#
# The grammar is represented by only terminals, non-terminals, epsilon (ε), '::=', and '|'.
@typechecked
class Grammar:
    nonterminals: SortedDict[Grammar.NonTerminal, Grammar.RuleSet]

    # A `Symbol` is either a terminal, a non-terminal, or special letter ε.
    class Symbol:
        token: str

        def __init__(self, token: str):
            self.token = token

        @override
        def __str__(self):
            return self.token

        @override
        def __hash__(self):
            return self.token.__hash__()

        @override
        def __eq__(self, o):
            return isinstance(o, Grammar.Symbol) and self.token.__eq__(o.token)

        @override
        def __lt__(self, o):
            if isinstance(o, Grammar.Symbol):
                return self.token.__lt__(o.token)
            raise NotImplementedError()

    class Epsilon(Symbol):
        def __init__(self):
            super().__init__('ε')

        @override
        def __repr__(self):
            return 'Epsilon()'

    class Terminal(Symbol):
        def __init__(self, token: str):
            super().__init__(token)

        @override
        def __repr__(self):
            return f'Terminal({self.token})'

    class NonTerminal(Symbol):
        def __init__(self, token: str):
            super().__init__(token)

        @override
        def __repr__(self):
            return f'NonTerminal({self.token})'

    # A `Rule` is simply a sequence of `Symbol`s.
    class Rule:
        sequence: tuple[Grammar.Symbol]

        def __init__(self, seq: list[Grammar.Symbol] = list()):
            for sym in seq:
                assert isinstance(sym, Grammar.Symbol)
            self.sequence = tuple(seq)  # list of 'Symbol's

        @override
        def __repr__(self):
            return 'Rule([' + ' '.join(map(repr, self.sequence)) + '])'

        @override
        def __str__(self):
            return ' '.join(map(str, self.sequence))

        @override
        def __len__(self):
            return len(self.sequence)

        @override
        def __iter__(self):
            return self.sequence.__iter__()

        @override
        def __reversed__(self):
            return reversed(self.sequence)

        @override
        def __hash__(self):
            return self.sequence.__hash__()

        @override
        def __eq__(self, o):
            return isinstance(o, Grammar.Rule) and self.sequence.__eq__(o.sequence)

        @override
        def __lt__(self, o):
            if isinstance(o, Grammar.Rule):
                return self.sequence.__lt__(o.sequence)
            raise NotImplementedError()

        def add(self, sym: Grammar.Symbol):
            self.sequence = self.sequence + (sym,)

    # A `RuleSet` is a `set` of `Rule`s.  It represents the alternative derivations for a
    # `NonTerminal`.
    class RuleSet:
        rules: SortedSet[Grammar.Rule]

        def __init__(self, rules: set[Grammar.Rule] = set()):
            for rule in rules:
                assert isinstance(rule, Grammar.Rule)
            self.rules = SortedSet(rules)

        @override
        def __repr__(self):
            return 'RuleSet([' + ' '.join(map(repr, self.rules)) + '])'

        @override
        def __str__(self):
            return ' |\n    '.join(map(str, self.rules)) + ' ;'

        @override
        def __iter__(self):
            return self.rules.__iter__()

        @override
        def __hash__(self):
            return self.rules.__hash__()

        @override
        def __eq__(self, o):
            return isinstance(o, Grammar.RuleSet) and self.rules.__eq__(o.rules)

        @override
        def __lt__(self, o):
            if isinstance(o, Grammar.RuleSet):
                return self.rules.__lt__(o.rules)
            raise NotImplementedError()

        def add(self, rule: Grammar.Rule):
            self.rules.add(copy.deepcopy(rule))

    def __init__(self):
        self.nonterminals = SortedDict()  # dict from 'NonTerminal' to 'RuleSet'
        self.nt_counter_ = 0

    @override
    def __str__(self):
        return '\n\n'.join(
            map(
                lambda entry: f'{entry[0]} ::=\n    {str(entry[1])}',
                self.nonterminals.items()
            )
        )

    @override
    def __iter__(self):
        return self.nonterminals.items().__iter__()

    def get_fresh_nonterminal(self, current_lhs: Grammar.NonTerminal) -> Grammar.NonTerminal:
        counter = 0
        s = current_lhs.token
        if s[0] != '_':
            s = f'_{s}'  # add prefix _
        while True:
            nt = Grammar.NonTerminal(f'{s}_{counter}')
            if nt not in self.nonterminals:
                self.nonterminals[nt] = None
                return nt
            counter += 1

    def add_rule(self, nonterminal: str, rule: Grammar.Rule):
        self.nonterminals.setdefault(nonterminal, list()).append(copy.deepcopy(rule))

    def add_rules(self, nt: Grammar.NonTerminal, rules: Grammar.RuleSet):
        assert self.nonterminals.get(nt, None) is None
        self.nonterminals[nt] = copy.deepcopy(rules)


# To translate from a grammar given in EBNF to our `Grammar` representation, some transformations are necessary.  These
# transformations are exemplified below:
#
#   Optional:
#   =========
#
#   To represent an optional, we introduce a fresh non-terminal with the contents of the optional *or* epsilon:
#
#       X  ::= A [ B C | D ] E
#
#   becomes
#
#       X  ::= A X' E
#       X' ::= B C | D | ε
#
#
#   Repetition:
#   ===========
#
#   To represent a repetition, we introduce a fresh non-terminal with the contents of the repetition and an optional
#   self recursion:
#
#       X  ::= A { B C | D } E
#
#   becomes
#
#       X  ::= A X' E
#       X' ::= X" X' | ε
#       X" ::= B C | D
#
#
#   Sequence:
#   =========
#
#   To represent a sequence, we introduce a fresh non-terminal with the contents of the sequence.
#
#       X  ::= A ( B C | D ) E
#
#   becomes
#
#       X  ::= A X' E
#       X' ::= B C | D
@typechecked
class EBNFParser:
    tok: str

    def __init__(self, stream):
        self.stream = stream
        self.consume()

    def consume(self):
        self.tok = next(self.stream, None)

    def accept(self, c: str) -> bool:
        if self.tok == c:
            self.consume()
            return True
        return False

    def expect(self, c: str):
        if self.tok == c:
            self.consume()
        else:
            print(f"Warning: expected token \"{c}\" but received token \"{self.tok}\".",
                  file=sys.stderr)

    def parse_Or(self, G: Grammar, current_lhs: Grammar.NonTerminal) -> Grammar.RuleSet:
        rules = Grammar.RuleSet()
        rules.add(self.parse_Seq(G, current_lhs))
        while self.accept('|'):
            rules.add(self.parse_Seq(G, current_lhs))
        return rules

    def parse_Seq(self, G: Grammar, current_lhs: Grammar.NonTerminal) -> Grammar.Rule:
        rule = Grammar.Rule()
        while self.tok:
            match self.tok:
                case '[':  # Optional
                    self.consume()

                    # X' ::= B C | D | ε
                    nt = G.get_fresh_nonterminal(current_lhs)
                    nt_rules = self.parse_Or(G, nt)
                    nt_rules.add(Grammar.Rule([Grammar.Epsilon()]))  # add alternative ε

                    G.add_rules(nt, nt_rules)
                    rule.add(nt)
                    self.expect(']')

                case '{':  # Repetition
                    self.consume()

                    # X" ::= B C | D
                    nt = G.get_fresh_nonterminal(current_lhs)
                    nt_rules = self.parse_Or(G, nt)

                    # X' ::= X" X' | ε
                    if len(nt_rules.rules) > 1:
                        G.add_rules(nt, nt_rules)
                        nt2 = G.get_fresh_nonterminal(current_lhs)
                        nt2_rules = Grammar.RuleSet()
                        nt2_rules.add(Grammar.Rule([nt, nt2]))
                        nt, nt_rules = nt2, nt2_rules
                    else:  # minor optimization: avoid superfluous nesting
                        nt_rules.rules[0].add(nt)
                    nt_rules.add(Grammar.Rule([Grammar.Epsilon()]))

                    G.add_rules(nt, nt_rules)
                    rule.add(nt)
                    self.expect('}')

                case '(':
                    self.consume()

                    # X' ::= B C | D
                    nt = G.get_fresh_nonterminal(current_lhs)
                    nt_rules = self.parse_Or(G, nt)

                    G.add_rules(nt, nt_rules)
                    rule.add(nt)
                    self.expect(')')

                case '(*':
                    while self.tok != '*)':
                        self.consume()
                    self.consume()

                case _:
                    if self.tok.isupper():  # terminal constant
                        rule.add(Grammar.Terminal(self.tok))
                        self.consume()
                    elif self.tok[0] == '\'':  # terminal string
                        assert self.tok[-1] == '\''
                        rule.add(Grammar.Terminal(self.tok))
                        self.consume()
                    elif self.tok.islower():  # non-terminal
                        rule.add(Grammar.NonTerminal(self.tok))
                        self.consume()
                    else:
                        # unrecognized symbol → end of sequence
                        break
        return rule

    def parse(self) -> Grammar:
        G = Grammar()

        while self.tok:
            # expect non-terminal
            if self.tok.islower():
                nt = Grammar.NonTerminal(self.tok)
                self.consume()
            else:
                print(f"ERROR: expected non-terminal but received {self.tok}.")
                return G

            self.expect('::=')
            rules = self.parse_Or(G, nt)
            G.add_rules(nt, rules)
            self.expect(';')
        return G


@typechecked
def read_grammar(filename: str) -> Grammar:
    with open(filename, 'rt') as file:
        stream = tokens(code_segments(file))
        Parser = EBNFParser(stream)
        return Parser.parse()


@typechecked
class FirstSet:
    first_map_t: TypeAlias = SortedDict[Grammar.NonTerminal, 'FirstSet']
    sequences: SortedSet[tuple[Grammar.Terminal]]

    def __init__(self, sequences=None):
        match sequences:
            case None:
                self.sequences = SortedSet([tuple()])
            case SortedSet():
                self.sequences = copy.copy(sequences)
            case set():
                self.sequences = SortedSet(sequences)
            case _:
                raise TypeError(f'Cannot create a SortedSet from {sequences}')

    @override
    def __repr__(self):
        return f'FirstSet({repr(self.sequences)})'

    @override
    def __str__(self):
        return '{ ' +\
               ', '.join(map(lambda seq: '(' + ' '.join(map(str, seq)) + ')', self.sequences)) +\
               ' }'

    def __iter__(self):
        return self.sequences.__iter__()

    def __eq__(self, o):
        if isinstance(o, FirstSet):
            return self.sequences.__eq__(o.sequences)
        return False

    def copy(self) -> FirstSet:
        return FirstSet(self.sequences)

    def without_epsilon(self) -> FirstSet:
        return FirstSet(set(filter(lambda seq: len(seq) > 0, self.sequences)))

    def exhausts_k(self, k: int) -> bool:
        return all(map(lambda seq: len(seq) >= k, self.sequences))

    def append_terminal(self, terminal: Grammar.Terminal, k: int) -> FirstSet:
        new_sequences: SortedSet[tuple[Grammar.Terminal]] = SortedSet()
        for seq in self:
            assert len(seq) <= k
            if len(seq) < k:
                new_sequences.add(seq + (terminal,))
            else:
                new_sequences.add(seq)  # unmodified
        self.sequences = new_sequences
        return self

    def append_set(self, other: FirstSet, k: int) -> FirstSet:
        assert len(self.sequences) >= 1  # must have at least ε
        assert len(other.sequences) >= 1  # must have at least ε
        new_sequences: SortedSet[tuple[Grammar.Terminal]] = SortedSet()
        for seq in self:
            assert len(seq) <= k
            if len(seq) < k:
                for other_seq in other:
                    if len(other_seq) > 0:
                        assert len(other_seq) <= k
                        cat = seq + other_seq
                        new_seq = cat[:k]
                        assert new_seq != seq
                        new_sequences.add(new_seq)
                    else:
                        new_sequences.add(seq)  # unmodified
            else:
                new_sequences.add(seq)  # unmodified
        self.sequences = new_sequences
        return self

    def add(self, other: FirstSet) -> FirstSet:
        for seq in other:
            self.sequences.add(seq)
        return self

    @staticmethod
    def ComputeFirst(G: Grammar, k: int = 1) -> FirstSet.first_map_t:
        F: SortedDict[Grammar.NonTerminal, FirstSet] = SortedDict()

        while True:
            F_new: SortedDict[Grammar.NonTerminal, FirstSet] = SortedDict()
            for nt, rules in G:
                for rule in rules:
                    #  print(f'Analyze rule {nt} → {rule}')
                    fs: FirstSet = FirstSet()
                    success: bool = True
                    for sym in rule:
                        if fs.exhausts_k(k):  # rule already converged to `k`
                            break  # break early

                        match sym:
                            case Grammar.Epsilon():
                                pass  # ignore epsilon

                            case Grammar.Terminal():
                                fs.append_terminal(sym, k)
                                for sym2 in fs:
                                    assert not isinstance(sym2, Grammar.Epsilon)

                            case Grammar.NonTerminal():
                                # As soon as we computed *some* first set for `sym`, we can already make use of it, even
                                # if it is not yet final/converged.
                                # - If `sym` contains no recursive rules, it converges immediately on first analysis.
                                # - If `sym` contains a recursive rule, it will not converge on the first analysis.
                                #   However, the partial results from the first analysis are already valid and can be
                                #   used.
                                if sym in F:
                                    fs.append_set(F[sym], k)
                                else:
                                    # If we have no first set for `sym` yet, we must stop here.  We will come back to
                                    # this rule later during the fixed point iteration.
                                    success = False
                                    break

                    if len(rule) > 1:
                        for sym2 in fs:
                            assert not isinstance(sym2, Grammar.Epsilon)
                    elif len(rule) == 1:
                        if isinstance(rule.sequence[0], Grammar.Epsilon):
                            assert fs.sequences[0] == tuple()

                    if success:
                        # Update first_sets[nt]
                        if nt in F_new:
                            #  print(f'  Update {nt} = {F_new[nt]} + {fs} = ', end='')
                            F_new[nt].add(fs)
                            #  print(F_new[nt])
                        else:
                            #  print(f'  Set {nt} := {fs}')
                            F_new[nt] = fs

            if F == F_new:
                break
            F = F_new

        return F

    @staticmethod
    def ComputeFollow(G: Grammar, first_sets: FirstSet.first_map_t, k: int = 1) -> FirstSet.first_map_t:
        F: SortedDict[Grammar.NonTerminal, FirstSet] = SortedDict()

        while True:
            F_new: SortedDict[Grammar.NonTerminal, FirstSet] = SortedDict()
            for nt, rules in G:
                for rule in rules:
                    fs: FirstSet | None = None  # current follow set
                    if nt in F:
                        fs = F[nt].copy()
                    for sym in reversed(rule):
                        match sym:
                            case Grammar.Epsilon():
                                pass  # ignore epsilon

                            case Grammar.Terminal():
                                #  print(f'    T {sym}')
                                fs_ = FirstSet().append_terminal(sym, k)
                                if fs is not None:
                                    fs_ = fs_.append_set(fs, k)
                                fs = fs_

                            case Grammar.NonTerminal():
                                #  print(f'    NT {sym}')
                                fs_ = first_sets[sym]
                                if fs is not None:
                                    #  print(f'      with follow set {fs}')
                                    # fs is a follow set of sym
                                    if sym in F_new:
                                        F_new[sym].add(fs.without_epsilon())
                                    else:
                                        F_new[sym] = fs.without_epsilon()
                                    fs_.append_set(fs, k)  # prepend first set of sym
                                fs = fs_

            if F == F_new:
                break
            F = F_new

        return F


@typechecked
class TokenTypeCache:
    token_cache: dict[Grammar.Terminal, set[str]]
    lex_binary: str

    def __init__(self, lex_binary: str):
        self.lex_binary = lex_binary

        # initialixe with special terminal tokens
        self.token_cache = {
            Grammar.Terminal('IDENTIFIER'): {'TK_IDENTIFIER'},
            Grammar.Terminal('INSTUCTION'): {'TK_INSTRUCTION'},
            Grammar.Terminal('STRING-LITERAL'): {'TK_STRING_LITERAL'},
            Grammar.Terminal('INTEGER-CONSTANT'): {'TK_DEC_INT', 'TK_OCT_INT', 'TK_HEX_INT'},
            Grammar.Terminal('DECIMAL-CONSTANT'): {'TK_DEC_INT'},
            Grammar.Terminal('CONSTANT'): {
                'TK_STRING_LITERAL',
                'TK_DEC_INT', 'TK_OCT_INT', 'TK_HEX_INT',
                'TK_DEC_FLOAT', 'TK_HEX_FLOAT',
                'TK_True', 'TK_False',
                'TK_DATE', 'TK_DATE_TIME'
            },
        }

    def get(self, term: Grammar.Terminal) -> set[str]:
        if term in self.token_cache:
            return self.token_cache[term]
        input = term.token[1:-1] if term.token[0] == '\'' else term.token
        p = subprocess.run(
            [self.lex_binary, '-'],
            input=input,
            text=True,
            capture_output=True,
            timeout=10,
            check=True
        )
        output = p.stdout.split()
        assert len(output) == 3  # (Position, str, TokenType)
        self.token_cache[term] = {output[2]}
        return {output[2]}


@typechecked
def tup2str(tup: tuple[Grammar.Terminal, ...]) -> set[str]:
    s: set[tuple[str, ...]] = {tuple()}
    for term in tup:
        tokens: set[str] = TKS.get(term)
        s = set([old + (tk,) for old in s for tk in tokens])
    return set(list(map(', '.join, s)))


@typechecked
def write_first_set(file: io.TextIOBase, nt: Grammar.NonTerminal, first: FirstSet, macro: str):
    nt_name = str(nt).upper().replace('-', '_')
    fs_strs: set[str] = reduce(set.union, map(tup2str, first))
    fs_str = ', '.join(map(lambda seq: f'{{ {seq} }}', fs_strs))
    print(f'{macro}({nt_name}, ({{ {fs_str} }}))', file=file)


########################################################################################################################
# MAIN
########################################################################################################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='FirstFollow',
        description='Computes first-k and follow-k sets of a grammar given in EBNF'
    )

    parser.add_argument(
        '-o', '--output', type=str, metavar='output',
        help='Output first/follow set to the given file',
    )
    parser.add_argument(
        '-k', type=int, default=1,
        help='Specify the size of the first/follow sets to compute',
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', dest='verbose',
        help='Be verbose',
    )
    parser.add_argument(
        '-g', '--grammar', action='store_true', dest='emit_grammar',
        help='Output the internal representation of the parsed grammar',
    )
    parser.add_argument(
        '--first', action='store_true', dest='emit_first',
        help='Output the first-k sets',
    )
    parser.add_argument(
        '--follow', action='store_true', dest='emit_follow',
        help='Output the follow-k sets',
    )
    parser.add_argument(
        '--build', type=str, metavar='build_dir',
        help='Specify the path to the build directory',
        default=DEFAULTS.BUILD_DIR,
    )
    parser.add_argument('filename', metavar='<Grammar file>', help='Grammar file in EBNF')

    args = parser.parse_args()

    print(f'Reading grammar from file \'{args.filename}\'')
    grammar = read_grammar(args.filename)

    if args.emit_grammar:
        print('(**** Grammar ****)')
        print(grammar)
        print('(**** End of grammar ****)')

    if not args.emit_first and not args.emit_follow:
        exit(0)  # we are done

    first_sets: FirstSet.first_map_t = FirstSet.ComputeFirst(grammar, args.k)
    TKS = TokenTypeCache(os.path.join(args.build, DEFAULTS.LEX_BINARY))

    out: io.TextIOBase = open(args.output, mode='wt') if args.output else sys.stdout

    print('/* vim: set filetype=cpp: */', file=out)

    if args.emit_first:
        print('''
#ifndef M_FIRST
#error "Must define M_FIRST before including this file."
#endif
''', file=out)
        for nt, first in first_sets.items():
            if args.verbose:
                print(f'{nt} → {first}')
            if not nt.token.startswith('_'):
                write_first_set(out, nt, first, 'M_FIRST')

    if args.emit_follow:
        follow_sets: FirstSet.first_map_t = FirstSet.ComputeFollow(grammar, first_sets, args.k)
        print('''
#ifndef M_FOLLOW
#error "Must define M_FOLLOW before including this file."
#endif
''', file=out)
        for nt, follow in follow_sets.items():
            if args.verbose:
                print(f'{nt} → {follow}')
            if not nt.token.startswith('_'):
                write_first_set(out, nt, follow, 'M_FOLLOW')

    out.close()


########################################################################################################################
# UNIT TESTS
########################################################################################################################

def test_Grammar():
    G = Grammar()
    ε = Grammar.Epsilon()
    term1 = Grammar.Terminal('T1')
    term2 = Grammar.Terminal('T2')
    nt1 = Grammar.NonTerminal('nt1')
    nt2 = Grammar.NonTerminal('nt2')

    rule1 = Grammar.Rule([term1, nt1, term2])
    assert str(rule1) == 'T1 nt1 T2'

    rule2 = Grammar.Rule()
    rule2.add(term2)
    rule2.add(nt1)
    rule2.add(nt1)
    assert str(rule2) == 'T2 nt1 nt1'

    rule3 = Grammar.Rule()
    rule3.add(ε)
    assert str(rule3) == 'ε'

    rs = Grammar.RuleSet([rule1, rule2])
    assert re.sub(r'\s+', ' ', str(rs)) == 'T1 nt1 T2 | T2 nt1 nt1 ;'

    G.add_rules(nt2, rs)
    assert re.sub(r'\s+', ' ', str(G)) == 'nt2 ::= T1 nt1 T2 | T2 nt1 nt1 ;'


def test_EBNFParser():
    input = '''\
a ::= A ( B C | D ) E ;

b ::= A [ B C | D ] E ;

c ::= A { B C | D } E ;

d ::= a { b } [ c ] ;
'''

    parser = EBNFParser(tokens(input.splitlines()))
    G = parser.parse()
    print(G)

    # Parsed grammar
    #
    #   _a_0 ::= B C | D ;
    #   a ::= A _a_0 E ;
    #
    #   _b_0 ::= B C | D | ε ;
    #   b ::= A _b_0 E ;
    #
    #   _c_0 ::= B C | D ;
    #   _c_1 ::= _c_0 _c_1 | ε ;
    #   c ::= A _c_1 E ;
    #
    #   _d_0 ::= c | ε ;
    #   _d_1 ::= b _d_1 | ε ;
    #   d ::= a _d_1 _d_0 ;

    _a_0 = Grammar.NonTerminal('_a_0')
    a = Grammar.NonTerminal('a')
    _b_0 = Grammar.NonTerminal('_b_0')
    b = Grammar.NonTerminal('b')
    _c_0 = Grammar.NonTerminal('_c_0')
    _c_1 = Grammar.NonTerminal('_c_1')
    c = Grammar.NonTerminal('c')
    _d_0 = Grammar.NonTerminal('_d_0')
    _d_1 = Grammar.NonTerminal('_d_1')
    d = Grammar.NonTerminal('d')

    # Rules for a
    assert _a_0 in G.nonterminals
    assert re.sub(r'\s+', ' ', str(G.nonterminals[_a_0])) == 'B C | D ;'
    assert a in G.nonterminals
    assert re.sub(r'\s+', ' ', str(G.nonterminals[a])) == 'A _a_0 E ;'

    # Rules for b
    assert _b_0 in G.nonterminals
    assert re.sub(r'\s+', ' ', str(G.nonterminals[_b_0])) == 'B C | D | ε ;'
    assert b in G.nonterminals
    assert re.sub(r'\s+', ' ', str(G.nonterminals[b])) == 'A _b_0 E ;'

    # Rules for c
    assert _c_0 in G.nonterminals
    assert re.sub(r'\s+', ' ', str(G.nonterminals[_c_0])) == 'B C | D ;'
    assert _c_1 in G.nonterminals
    assert re.sub(r'\s+', ' ', str(G.nonterminals[_c_1])) == '_c_0 _c_1 | ε ;'
    assert c in G.nonterminals
    assert re.sub(r'\s+', ' ', str(G.nonterminals[c])) == 'A _c_1 E ;'

    # Rules for d
    assert _d_0 in G.nonterminals
    assert re.sub(r'\s+', ' ', str(G.nonterminals[_d_0])) == 'b _d_0 | ε ;'
    assert _d_1 in G.nonterminals
    assert re.sub(r'\s+', ' ', str(G.nonterminals[_d_1])) == 'c | ε ;'
    assert d in G.nonterminals
    assert re.sub(r'\s+', ' ', str(G.nonterminals[d])) == 'a _d_0 _d_1 ;'

    assert len(G.nonterminals) == 10


def test_ComputeFirst():
    input = '''\
a ::= A ( B C | D ) E ;

b ::= A [ B C | D ] E ;

c ::= A { B C | D } E ;

d ::= { a } b [ c ] ;

x ::= x A y | x B y | y ;

y ::= y C | y D | E | '(' x ')' ;
'''

    # Parsed grammar
    #
    #   _a_0 ::= B C | D ;
    #   a ::= A _a_0 E ;
    #
    #   _b_0 ::= B C | D | ε ;
    #   b ::= A _b_0 E ;
    #
    #   _c_0 ::= B C | D ;
    #   _c_1 ::= _c_0 _c_1 | ε ;
    #   c ::= A _c_1 E ;
    #
    #   _d_0 ::= a _d_0 | ε ;
    #   _d_1 ::= c | ε ;
    #   d ::= _d_0 b _d_1 ;
    #
    #   x ::= x A y | x B y | y ;
    #
    #   y ::= y C | y D | E | '(' x ')' ;

    A = Grammar.Terminal('A')
    B = Grammar.Terminal('B')
    C = Grammar.Terminal('C')
    D = Grammar.Terminal('D')
    E = Grammar.Terminal('E')
    lpar = Grammar.Terminal('\'(\'')
    rpar = Grammar.Terminal('\')\'')

    _a_0 = Grammar.NonTerminal('_a_0')
    a = Grammar.NonTerminal('a')
    _b_0 = Grammar.NonTerminal('_b_0')
    b = Grammar.NonTerminal('b')
    _c_0 = Grammar.NonTerminal('_c_0')
    _c_1 = Grammar.NonTerminal('_c_1')
    c = Grammar.NonTerminal('c')
    _d_0 = Grammar.NonTerminal('_d_0')
    _d_1 = Grammar.NonTerminal('_d_1')
    d = Grammar.NonTerminal('d')
    x = Grammar.NonTerminal('x')
    y = Grammar.NonTerminal('y')

    parser = EBNFParser(tokens(input.splitlines()))
    G = parser.parse()

    # first-1
    first1 = FirstSet.ComputeFirst(G, k=1)
    assert _a_0 in first1
    assert first1[_a_0] == FirstSet({(B,), (D,)})
    assert a in first1
    assert first1[a] == FirstSet({(A,)})
    assert _b_0 in first1
    assert first1[_b_0] == FirstSet({(B,), (D,), tuple()})
    assert b in first1
    assert first1[b] == FirstSet({(A,)})
    assert _c_0 in first1
    assert first1[_c_0] == FirstSet({(B,), (D,)})
    assert _c_1 in first1
    assert first1[_c_1] == FirstSet({(B,), (D,), tuple()})
    assert c in first1
    assert first1[c] == FirstSet({(A,)})
    assert _d_0 in first1
    assert first1[_d_0] == FirstSet({(A,), tuple()})
    assert _d_1 in first1
    assert first1[_d_1] == FirstSet({(A,), tuple()})
    assert d in first1
    assert first1[d] == FirstSet({(A,)})
    assert x in first1
    assert first1[x] == FirstSet({(E,), (lpar,)})
    assert y in first1
    assert first1[y] == FirstSet({(E,), (lpar,)})

    # first-3
    first3 = FirstSet.ComputeFirst(G, k=3)
    assert _a_0 in first3
    assert first3[_a_0] == FirstSet({(B, C), (D,)})
    assert a in first3
    assert first3[a] == FirstSet({(A, B, C), (A, D, E)})
    assert _b_0 in first3
    assert first3[_b_0] == FirstSet({(B, C), (D,), tuple()})
    assert b in first3
    assert first3[b] == FirstSet({(A, E), (A, B, C), (A, D, E)})
    assert _c_0 in first3
    assert first3[_c_0] == FirstSet({(B, C), (D,)})
    assert _c_1 in first3
    assert first3[_c_1] == FirstSet({
        tuple(),
        (D,),
        (B, C), (D, D),
        (B, C, B), (B, C, D), (D, B, C), (D, D, B), (D, D, D),
    })
    assert c in first3
    assert first3[c] == FirstSet({
        (A, E),
        (A, B, C), (A, D, B), (A, D, D), (A, D, E),
    })
    assert _d_0 in first3
    assert first3[_d_0] == FirstSet({
        tuple(),
        (A, B, C), (A, D, E),
    })
    assert _d_1 in first3
    assert first3[_d_1] == FirstSet({
        tuple(),
        (A, E),
        (A, B, C), (A, D, B), (A, D, D), (A, D, E),
    })
    assert d in first3
    assert first3[d] == FirstSet({
        (A, B, C), (A, D, E),  # from a (and ε b)
        (A, E),  # from b
        (A, E, A),  # from ε b _d_1
    })
    assert x in first3
    assert first3[x] == FirstSet({
        (lpar, lpar, lpar), (lpar, lpar, E),
        (lpar, E, rpar),
        (lpar, E, A), (lpar, E, B),
        (lpar, E, C), (lpar, E, D),
        (E,),
        (E, A, lpar), (E, A, E), (E, B, lpar), (E, B, E),
        (E, C), (E, C, A), (E, C, B), (E, C, C), (E, C, D),
        (E, D), (E, D, A), (E, D, B), (E, D, C), (E, D, D),
    })
    assert y in first3
    assert first3[y] == FirstSet({
        (lpar, lpar, lpar), (lpar, lpar, E),
        (lpar, E, rpar),
        (lpar, E, A), (lpar, E, B),
        (lpar, E, C), (lpar, E, D),
        (E,),
        (E, C), (E, C, C), (E, C, D),
        (E, D), (E, D, C), (E, D, D),
    })


def test_ComputeFollow():
    input = '''\
a ::= A ( B C | D ) E ;

b ::= A [ B C | D ] E ;

c ::= A { B C | D } E ;

d ::= { a } b [ c ] ;

x ::= x A y | x B y | y ;

y ::= y C | y D | E | '(' x ')' ;
'''

    # Parsed grammar
    #
    #   _a_0 ::= B C | D ;
    #   a ::= A _a_0 E ;
    #
    #   _b_0 ::= B C | D | ε ;
    #   b ::= A _b_0 E ;
    #
    #   _c_0 ::= B C | D ;
    #   _c_1 ::= _c_0 _c_1 | ε ;
    #   c ::= A _c_1 E ;
    #
    #   _d_0 ::= a _d_0 | ε ;
    #   _d_1 ::= c | ε ;
    #   d ::= _d_0 b _d_1 ;
    #
    #   x ::= x A y | x B y | y ;
    #
    #   y ::= y C | y D | E | '(' x ')' ;

    A = Grammar.Terminal('A')
    B = Grammar.Terminal('B')
    C = Grammar.Terminal('C')
    D = Grammar.Terminal('D')
    E = Grammar.Terminal('E')
    rpar = Grammar.Terminal('\')\'')

    _a_0 = Grammar.NonTerminal('_a_0')
    a = Grammar.NonTerminal('a')
    _b_0 = Grammar.NonTerminal('_b_0')
    b = Grammar.NonTerminal('b')
    _c_0 = Grammar.NonTerminal('_c_0')
    _c_1 = Grammar.NonTerminal('_c_1')
    c = Grammar.NonTerminal('c')
    _d_0 = Grammar.NonTerminal('_d_0')
    _d_1 = Grammar.NonTerminal('_d_1')
    d = Grammar.NonTerminal('d')
    x = Grammar.NonTerminal('x')
    y = Grammar.NonTerminal('y')

    parser = EBNFParser(tokens(input.splitlines()))
    G = parser.parse()
    first1 = FirstSet.ComputeFirst(G, k=1)

    # follow-1
    follow1 = FirstSet.ComputeFollow(G, first1, k=1)
    assert _a_0 in follow1
    assert follow1[_a_0] == FirstSet({(E,)})
    assert a in follow1
    assert follow1[a] == FirstSet({(A,)})
    assert _b_0 in follow1
    assert follow1[_b_0] == FirstSet({(E,)})
    assert b in follow1
    assert follow1[b] == FirstSet({(A,)})
    assert _c_0 in follow1
    assert follow1[_c_0] == FirstSet({(B,), (D,), (E,)})
    assert _c_1 in follow1
    assert follow1[_c_1] == FirstSet({(E,)})
    assert c not in follow1  # no follow set
    assert _d_0 in follow1
    assert follow1[_d_0] == FirstSet({(A,)})
    assert _d_1 not in follow1
    assert d not in follow1
    assert x in follow1
    assert follow1[x] == FirstSet({(A,), (B,), (rpar,)})
    assert y in follow1
    assert follow1[y] == FirstSet({(C,), (D,), (A,), (B,), (rpar,)})
