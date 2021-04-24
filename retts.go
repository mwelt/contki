package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

// Utility {{{

func max(x, y int) int {
	if x >= y {
		return x
	}
	return y
}

// }}}

// Vocabulary {{{

type Constant string
type Variable string

type Literal interface {
	getLiteralType() uint8
}

const (
	LONG   = iota
	DOUBLE = iota
	STRING = iota
	ARRAY  = iota
)

type Long uint64
type Double uint64
type String string
type Array []Literal

func (l Long) getLiteralType() uint8   { return LONG }
func (d Double) getLiteralType() uint8 { return DOUBLE }
func (s String) getLiteralType() uint8 { return STRING }
func (a Array) getLiteralType() uint8  { return ARRAY }

const (
	CONSTANT = iota
	VARIABLE = iota
	LITERAL  = iota
)

type Term interface {
	getTermType() uint8
}

func (c Constant) getTermType() uint8 { return CONSTANT }
func (v Variable) getTermType() uint8 { return VARIABLE }
func (l Long) getTermType() uint8     { return LITERAL }
func (l Double) getTermType() uint8   { return LITERAL }
func (l String) getTermType() uint8   { return LITERAL }
func (l Array) getTermType() uint8    { return LITERAL }

func isConstant(t Term) bool { return t.getTermType() == CONSTANT }
func isVariable(t Term) bool { return t.getTermType() == VARIABLE }
func isLiteral(t Term) bool  { return t.getTermType() == LITERAL }

type Atom struct {
	idx     int
	s, p, o Term
}

func (a *Atom) isGround() bool {
	return isConstant(a.s) && isConstant(a.p) && isConstant(a.o)
}

func (a1 *Atom) equalTo(a2 *Atom) bool {
	if !a1.isGround() || !a2.isGround() {
		return false
	}

	return a1.s.(Constant) == a2.s.(Constant) && a1.p.(Constant) == a2.p.(Constant) && a1.o.(Constant) == a2.o.(Constant)
}

// }}}

// Store {{{

type Node struct {
	sPos, pPos, oPos []int
}

type Store struct {
	abox  []Atom
	index map[Constant]Node
}

func (s *Store) addAtom(a Atom) {
	a.idx = len((*s).abox)
	(*s).abox = append((*s).abox, a)

	sNode, ok1 := (*s).index[a.s.(Constant)]
	if !ok1 {
		sNode = Node{}
	}
	sNode.sPos = append(sNode.sPos, a.idx)
	(*s).index[a.s.(Constant)] = sNode

	pNode, ok2 := (*s).index[a.p.(Constant)]
	if !ok2 {
		pNode = Node{}
	}
	pNode.pPos = append(pNode.pPos, a.idx)
	(*s).index[a.p.(Constant)] = pNode

	oNode, ok3 := (*s).index[a.o.(Constant)]
	if !ok3 {
		oNode = Node{}
	}
	oNode.oPos = append(oNode.oPos, a.idx)
	(*s).index[a.o.(Constant)] = oNode
}

// }}}

// Mu / Omega {{{
type MuMapping map[Variable]Term

type Mu struct {
	idx     int
	mapping MuMapping
}

// compatible checks if mu1 is compatible to mu2
func (m1 *Mu) compatible(m2 *Mu) bool {
	for k, v := range (*m1).mapping {
		v_, ok := (*m2).mapping[k]
		if ok && v != v_ {
			return false
		}
	}
	return true
}

// join joins two mu mappings together
func (m1 *Mu) join(m2 *Mu) Mu {
	m3 := make(MuMapping)
	for k, v := range (*m1).mapping {
		m3[k] = v
	}
	for k, v := range (*m2).mapping {
		_, ok := m3[k]
		if !ok {
			m3[k] = v
		}
	}

	return Mu{max(m1.idx, m2.idx), m3}
}

type Omega []Mu

// join joins two multisets o1 and o2 together based on mu
// compatibility
func (o1 *Omega) joinPar(o2 *Omega) Omega {

	c := make(chan []Mu)

	for _, mu1 := range *o1 {
		go func(mu1 Mu) {
			o3Part := make([]Mu, 0, len(*o2))
			for _, mu2 := range *o2 {
				if mu1.compatible(&mu2) {
					o3Part = append(o3Part, mu1.join(&mu2))
				}
			}
			c <- o3Part
		}(mu1)
	}

	o3 := make(Omega, 0, len(*o1)+len(*o2))

	for i := 0; i < len(*o1); i++ {
		o3 = append(o3, <-c...)
	}

	return o3
}

func (o1 *Omega) join(o2 *Omega) Omega {
	o3 := make(Omega, 0, len(*o1)+len(*o2))

	for _, mu1 := range *o1 {
		for _, mu2 := range *o2 {
			if mu1.compatible(&mu2) {
				o3 = append(o3, mu1.join(&mu2))
			}
		}
	}

	return o3
}

// }}}

// Bgp {{{

// TODO: Discuss if a ground atom can be a BGP. With respect to
// literature this is possible.
func (a *Atom) isBgp() bool {
	return isVariable(a.s) || isVariable(a.p) || isVariable(a.o)
}

// matches tests if a bgp matches a ground atom
func (bgp *Atom) matches(a *Atom) bool {
	if (isConstant(bgp.s) && bgp.s == a.s || isVariable(bgp.s)) && (isConstant(bgp.p) && bgp.p == a.p || isVariable(bgp.p)) && (isConstant(bgp.o) && bgp.o == a.o || isVariable(bgp.o)) {
		return true
	}
	return false
}

// toMu creates a mapping mu from a bgp and a matching ground atom
func (bgp *Atom) toMu(a *Atom) Mu {
	mu := make(MuMapping)
	if isVariable(bgp.s) {
		mu[bgp.s.(Variable)] = a.s
	}
	if isVariable(bgp.p) {
		mu[bgp.p.(Variable)] = a.p
	}
	if isVariable(bgp.o) {
		mu[bgp.o.(Variable)] = a.o
	}
	return Mu{a.idx, mu}
}

// applyMapping creates a ground atom from an non ground atom and a
// corresponding mu mapping
func (a *Atom) applyMapping(mu *Mu) Atom {
	ga := Atom{}

	if isVariable(a.s) {
		ga.s = (*mu).mapping[a.s.(Variable)]
	} else {
		ga.s = a.s.(Constant)
	}
	if isVariable(a.p) {
		ga.p = (*mu).mapping[a.p.(Variable)]
	} else {
		ga.p = a.p.(Constant)
	}
	if isVariable(a.o) {
		ga.o = (*mu).mapping[a.o.(Variable)]
	} else {
		ga.o = a.o.(Constant)
	}

	return ga
}

// findMappings finds all mappings in an abox (i.e. list of ground
// atoms) corresponding to graph pattern bgp
func (bgp *Atom) findMappings(abox *[]Atom) Omega {
	omega := make(Omega, 0, 100)

	// TODO use index structure
	for _, a := range *abox {
		if bgp.matches(&a) {
			omega = append(omega, bgp.toMu(&a))
		}
	}

	return omega
}

func (a1 *Atom) knownTo(abox *[]Atom) bool {

	// TODO use index structure
	for _, a2 := range *abox {
		if a1.equalTo(&a2) {
			return true
		}
	}
	return false
}

// }}}

// Rule {{{

type Rule struct {
	head, body []Atom
}

func (r *Rule) isFact() bool {
	return len(r.body) == 0
}

// eval evaluates a rule w.r.t. to an abox, and returns a multiset omega
func (r *Rule) eval(abox *[]Atom) Omega {
	omegas := make([]Omega, 0)

	// TODO parallel?
	for _, b := range (*r).body {
		omegas = append(omegas, b.findMappings(abox))
	}

	result := omegas[0]

	start := time.Now()
	for i := 1; i < len(omegas); i++ {
		result = result.joinPar(&omegas[i])
	}
	elapsed := time.Since(start)
	fmt.Println("join", elapsed)

	return result
}

// }}}

// naive Datalog evaluation {{{

type Stats struct {
	cmps, iters int
}

// eval returns a list of ground atoms, considered to be a
// delta of one cycle of rule application. A rule will only add facts
// to delta, that result by applying at least one ground atom with id
// >= lastCommit, in order to make shure, which atom has actually
// derived new facts
func eval(tbox *[]Rule, abox *[]Atom, lastDelta int, stats *Stats) []Atom {

	delta := make([]Atom, 0)

	// TODO parallel!
	for _, r := range *tbox {
		omega := r.eval(abox)
		for _, mu := range omega {
			// only add mu's that utilize a fact from the last delta,
			// i.e. utilize a ground atom with an index >= lastDelta
			if mu.idx >= lastDelta {
				// apply mu to all head atoms of r
				for _, headAtom := range r.head {
					ga := headAtom.applyMapping(&mu)
					stats.cmps += 1
					if !ga.knownTo(abox) {
						ga.idx = len(*abox) + len(delta)
						delta = append(delta, ga)
					}
				}
			}
		}
	}

	return delta

}

func naiveDatalog(tbox *[]Rule, abox *[]Atom) Stats {

	stats := Stats{0, 0}
	lastDelta := 0
	currDelta := len(*abox)

	for lastDelta < currDelta {
		delta := eval(tbox, abox, lastDelta, &stats)
		*abox = append(*abox, delta...)
		lastDelta = currDelta
		currDelta = len(*abox)
		stats.iters += 1
	}

	return stats

}

//

// main {{{

func main() {

	// TBox
	r1 := Rule{
		head: []Atom{
			Atom{-1, Variable("?x"), Constant(":reachable"), Variable("?y")}},
		body: []Atom{
			Atom{-1, Variable("?x"), Constant(":link"), Variable("?y")}}}

	r2 := Rule{
		head: []Atom{
			Atom{-1, Variable("?x"), Constant(":reachable"), Variable("?y")}},
		body: []Atom{
			Atom{-1, Variable("?x"), Constant(":link"), Variable("?z")},
			Atom{-1, Variable("?z"), Constant(":reachable"), Variable("?y")}}}

	// // goal
	// goal := Rule{
	// 	head: []Atom{
	// 		Atom{-1, Constant(":goal"), Constant(":state"), Constant(":success")}},
	// 	body: []Atom{
	// 		Atom{-1, Constant(":a"), Constant(":reachable"), Constant(":c")}}}

	tbox := []Rule{r1, r2}

	// ABox
	// abox := []Atom{
	// 	Atom{0, Constant(":a"), Constant(":link"), Constant(":b")},
	// 	Atom{1, Constant(":b"), Constant(":link"), Constant(":c")},
	// 	Atom{2, Constant(":c"), Constant(":link"), Constant(":d")},
	// 	Atom{3, Constant(":c"), Constant(":link"), Constant(":c")}}

	abox := genRngAbox(10000, 5000)

	// fmt.Println("TBox:")
	// for _, r := range tbox {
	// 	fmt.Println(r)
	// }

	// fmt.Println("ABox:")
	// for _, r := range abox {
	// 	fmt.Println(r)
	// }

	fmt.Println("starting naive Datalog evaluation")

	start := time.Now()
	stats1 := naiveDatalog(&tbox, &abox)
	elapsed := time.Since(start)
	fmt.Println("finished naive Datalog evaluation, took", elapsed)
	fmt.Println("final abox size:", len(abox), "stats:", stats1)
	// fmt.Println("final ABox:")
	// for _, a := range abox {
	// 	fmt.Println(a)
	// }
	// stats2 := naiveDatalog(&tbox, &abox)
	// fmt.Println("stats:", stats2)
	// fmt.Println("final ABox:")
	// for _, a := range abox {
	// 	fmt.Println(a)
	// }
}

func genRngAbox(numNodes, numAtoms int) []Atom {
	abox := make([]Atom, 0, numAtoms)
	count := 0

	for i := 0; i < numAtoms; i++ {
		n1 := rand.Intn(numNodes)
		n2 := rand.Intn(numNodes)
		a := Atom{count, Constant(":n" + strconv.Itoa(n1)), Constant(":link"), Constant(":n" + strconv.Itoa(n2))}
		abox = append(abox, a)
		count += 1
	}

	return abox
}

// }}}
