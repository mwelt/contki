package main

import "fmt"

// Vocabulary {{{

type Constant string
type Variable string

// type Literal interface {
// 	getLiteralType() uint8
// }

// const (
// 	LONG   = iota
// 	DOUBLE = iota
// 	STRING = iota
// 	ARRAY  = iota
// )

// type Long uint64
// type Double uint64
// type String string
// type Array []Literal

// func (l Long) getLiteralType() uint8   { return LONG }
// func (d Double) getLiteralType() uint8 { return DOUBLE }
// func (s String) getLiteralType() uint8 { return STRING }
// func (a Array) getLiteralType() uint8  { return ARRAY }

const (
	CONSTANT = iota
	VARIABLE = iota
	// LITERAL  = iota
)

type Term interface {
	getTermType() uint8
}

func (c Constant) getTermType() uint8 { return CONSTANT }
func (v Variable) getTermType() uint8 { return VARIABLE }

// func (l Long) getTermType() uint8     { return LITERAL }
// func (l Double) getTermType() uint8   { return LITERAL }
// func (l String) getTermType() uint8   { return LITERAL }
// func (l Array) getTermType() uint8    { return LITERAL }

func isConstant(t Term) bool { return t.getTermType() == CONSTANT }
func isVariable(t Term) bool { return t.getTermType() == VARIABLE }

// func isLiteral(t Term) bool  { return t.getTermType() == LITERAL }

type Atom struct {
	s, p, o Term
	neg     bool
}

func newAtom(s, p, o string) Atom {
	a := Atom{neg: false}
	switch s[0] {
	case ':':
		a.s = Constant(s)
	case '?':
		a.s = Variable(s)
	default:
		panic("only constant or variable allowed in s position")
	}
	switch p[0] {
	case ':':
		a.p = Constant(p)
	// case '?':
	// 	a.p = Variable(p)
	default:
		panic("only constant allowed in p position")
	}
	switch o[0] {
	case ':':
		a.o = Constant(o)
	case '?':
		a.o = Variable(o)
	default:
		panic("only constant or variable allowed in o position")
	}

	return a
}

func newNegAtom(s, p, o string) Atom {
	a := newAtom(s, p, o)
	a.neg = true
	return a
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

// Database {{{

type Database struct {
	idb     map[Constant][]Atom
	edb     map[Constant][]Atom
	commits map[Constant][]int
}

func newDatabase() Database {
	return Database{
		idb:     make(map[Constant][]Atom),
		edb:     make(map[Constant][]Atom),
		commits: make(map[Constant][]int),
	}
}

func (d *Database) shallowCopy() Database {
	d_ := newDatabase()

	for k, _ := range d.idb {
		d_.registerIdbRel(k)
	}

	for k, _ := range d.edb {
		d_.registerEdbRel(k)
	}

	return d_

}

func (d *Database) deepCopy() Database {

	d_ := d.shallowCopy()

	for relName, rel := range d.idb {
		d_.idb[relName] = append(d_.idb[relName], rel...)
	}

	for relName, rel := range d.edb {
		d_.edb[relName] = append(d_.edb[relName], rel...)
	}

	for relName, cs := range d.commits {
		d_.commits[relName] = append(d_.commits[relName], cs...)
	}

	return d_

}

func relsEqualTo(rels, rels_ *map[Constant][]Atom) bool {
	for relName, rel := range *rels {
		rel_, ok := (*rels_)[relName]

		if !ok || len(rel_) != len(rel) {
			return false
		}

		for _, a := range rel {
			if !relKnows(rel_, a) {
				return false
			}
		}
	}
	return true
}

func (d *Database) equalTo(d_ *Database) bool {
	return relsEqualTo(&(*d).idb, &(*d_).idb) && relsEqualTo(&(*d).edb, &(*d_).edb)
}

func (d *Database) empty() bool {
	for _, v := range d.idb {
		if len(v) > 0 {
			return false
		}
	}

	for _, v := range d.edb {
		if len(v) > 0 {
			return false
		}
	}

	return true
}

func (d *Database) commitRel(rel *map[Constant][]Atom) {
	for relName, rel := range *rel {
		(*d).commits[relName] = append((*d).commits[relName], len(rel))
	}
}

func (d *Database) commit() {
	d.commitRel(&(*d).idb)
	d.commitRel(&(*d).edb)
}

func (d *Database) revertRel(rels *map[Constant][]Atom) {
	for relName, rel := range *rels {
		l := len((*d).commits[relName])
		if l > 0 {
			l_ := (*d).commits[relName][l-1]
			(*d).commits[relName] = (*d).commits[relName][:l-1]
			(*rels)[relName] = rel[:l_]
		}
	}
}

func (d *Database) revert() {
	d.revertRel(&(*d).idb)
	d.revertRel(&(*d).edb)
}

func appendRels(rels, rels_ *map[Constant][]Atom, checkDoublette bool) {
	for relName, rel := range *rels {
		rel_, ok := (*rels_)[relName]
		if ok {
			if checkDoublette {
				rel__ := make([]Atom, 0, len(rel_))
				for _, a := range rel_ {
					if !relKnows(rel, a) {
						rel__ = append(rel__, a)
					}
				}
				(*rels)[relName] = append(rel, rel__...)
			} else {
				(*rels)[relName] = append(rel, rel_...)
			}
		}
	}
}

func (d *Database) append(d_ *Database, checkDoublette bool) {
	appendRels(&(*d).idb, &(*d_).idb, checkDoublette)
	appendRels(&(*d).edb, &(*d_).edb, checkDoublette)
}

func removeRels(rels, rels_ *map[Constant][]Atom) {
	for relName, rel := range *rels {
		rel_, ok := (*rels_)[relName]
		if ok {
			rel__ := make([]Atom, 0, len(rel))
			for _, a := range rel {
				if !relKnows(rel_, a) {
					rel__ = append(rel__, a)
				}
			}
			(*rels)[relName] = rel__
		}
	}
}

func (d *Database) clearIdb() {
	for relName, _ := range (*d).idb {
		(*d).idb[relName] = make([]Atom, 0)
	}
}

func (d *Database) remove(d_ *Database) {
	removeRels(&(*d).idb, &(*d_).idb)
	removeRels(&(*d).edb, &(*d_).edb)
}

func dumpRels(rels *map[Constant][]Atom) {
	for relName, rel := range *rels {
		fmt.Println("\t", relName)
		for _, a := range rel {
			fmt.Println("\t", a)
		}
		fmt.Println("")
	}
}

func (d *Database) dump() {
	fmt.Println("EDB:")
	dumpRels(&(*d).edb)
	fmt.Println("IDB:")
	dumpRels(&(*d).idb)
}

func (d *Database) addAtom(a Atom) {

	if !a.isGround() {
		panic("only ground atoms can be added to the database")
	}

	if d.isEdbRelation(a.p.(Constant)) {
		rel, _ := (*d).edb[a.p.(Constant)]
		rel = append(rel, a)
		(*d).edb[a.p.(Constant)] = rel
	} else if d.isIdbRelation(a.p.(Constant)) {
		rel, _ := (*d).idb[a.p.(Constant)]
		rel = append(rel, a)
		(*d).idb[a.p.(Constant)] = rel
	} else {
		d.registerEdbRel(a.p.(Constant))
		rel, _ := (*d).edb[a.p.(Constant)]
		rel = append(rel, a)
		(*d).edb[a.p.(Constant)] = rel
	}

}

func (d *Database) registerEdbRel(c Constant) {
	if d.isIdbRelation(c) {
		panic("relation already registered as idb relation")
	}

	_, ok := d.edb[c]

	if !ok {
		d.edb[c] = make([]Atom, 0)
		d.commits[c] = make([]int, 0)
	}
}

func (d *Database) registerIdbRel(c Constant) {
	if d.isEdbRelation(c) {
		panic("relation already registered as edb relation")
	}

	_, ok := d.idb[c]

	if !ok {
		d.idb[c] = make([]Atom, 0)
		d.commits[c] = make([]int, 0)
	}
}

func (d *Database) isIdbRelation(c Constant) bool {
	_, ok := d.idb[c]
	return ok
}

func (d *Database) isEdbRelation(c Constant) bool {
	_, ok := d.edb[c]
	return ok
}

// findMappings finds all mappings in an abox (i.e. list of ground
// atoms) corresponding to graph pattern bgp
func (db *Database) findMappingsFor(bgp *Atom) Omega {
	omega := make(Omega, 0, 100)

	if !isConstant(bgp.p) {
		panic("only bgp with constant predicate are currently supported.")
	}

	rel, ok := db.idb[bgp.p.(Constant)]
	if !ok {
		rel, ok = db.edb[bgp.p.(Constant)]
		if !ok {
			return omega
		}
	}

	for _, a := range rel {
		if bgp.matches(&a) {
			omega = append(omega, bgp.toMu(&a))
		}
	}

	return omega
}

func relKnows(rel []Atom, a Atom) bool {
	for _, a_ := range rel {
		if a.equalTo(&a_) {
			return true
		}
	}
	return false
}

func (db *Database) knows(a Atom) bool {

	if !a.isGround() {
		return false
	}

	rel, ok := db.idb[a.p.(Constant)]
	if !ok {
		rel, ok = db.edb[a.p.(Constant)]
		if !ok {
			return false
		}
	}

	return relKnows(rel, a)
}

// }}}

// Bgp {{{

// matches tests if a bgp matches a ground atom
func (bgp *Atom) matches(a *Atom) bool {

	// if any of the bgp is constant and does not match the
	// corresponding atom part return false
	if isConstant(bgp.s) && bgp.s != a.s || isConstant(bgp.p) && bgp.p != a.p || isConstant(bgp.o) && bgp.o != a.o {
		return false
	}

	// if a is variable, and p or o happens to point at the
	// same variable, but the atom is different return false
	if isVariable(bgp.s) {
		if bgp.s == bgp.p && a.s != a.p || bgp.s == bgp.o && a.s != a.o {
			return false
		}
	}

	// if p is variable, and a happens to point at the same
	// variable, but the atom is different return false
	if isVariable(bgp.p) && bgp.p == bgp.o && a.p != a.o {
		return false
	}

	return true

}

// toMu creates a mapping mu from a bgp and a matching ground atom
func (bgp *Atom) toMu(a *Atom) Mu {

	if bgp.isGround() {
		panic("can not create a mapping of a ground atom!")
	}

	mu := make(Mu)

	if isVariable(bgp.s) {
		mu[bgp.s.(Variable)] = a.s
	}

	if isVariable(bgp.p) {
		mu[bgp.p.(Variable)] = a.p
	}

	if isVariable(bgp.o) {
		mu[bgp.o.(Variable)] = a.o
	}

	return mu
}

// applyMapping creates a ground atom from an non ground atom and a
// corresponding mu mapping
func (a *Atom) applyMapping(mu *Mu) Atom {

	if a.isGround() {
		panic("can not apply a mapping to a ground atom!")
	}

	ga := Atom{}

	if isVariable(a.s) {
		ga.s = (*mu)[a.s.(Variable)]
	} else {
		ga.s = a.s.(Constant)
	}

	if isVariable(a.p) {
		ga.p = (*mu)[a.p.(Variable)]
	} else {
		ga.p = a.p.(Constant)
	}

	if isVariable(a.o) {
		ga.o = (*mu)[a.o.(Variable)]
	} else {
		ga.o = a.o.(Constant)
	}

	return ga
}

// }}}

// Mu / Omega / Join {{{

type Mu map[Variable]Term

// compatible checks if mu1 is compatible to mu2
func (m1 *Mu) compatible(m2 *Mu) bool {
	for k, v := range *m1 {
		v_, ok := (*m2)[k]
		if ok && v != v_ {
			return false
		}
	}
	return true
}

func (m1 *Mu) negCompatible(m2 *Mu) bool {

	for k, v := range *m2 {
		v_, ok := (*m1)[k]
		if ok {
			if v != v_ {
				return true
			}
		} else {
			return true
		}
	}

	return false
}

// join joins two mu mappings together
func (m1 *Mu) join(m2 *Mu) Mu {
	m3 := make(Mu)
	for k, v := range *m1 {
		m3[k] = v
	}
	for k, v := range *m2 {
		_, ok := m3[k]
		if !ok {
			m3[k] = v
		}
	}

	return m3
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

func (o1 *Omega) joinNeg(o2 *Omega) Omega {
	o3 := make(Omega, 0, len(*o1)+len(*o2))

	for _, mu1 := range *o1 {
		allNegCompatible := true
		for _, mu2 := range *o2 {
			if !mu1.negCompatible(&mu2) {
				allNegCompatible = false
				break
			}
		}
		if allNegCompatible {
			o3 = append(o3, mu1)
		}
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
