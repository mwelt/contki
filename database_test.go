package main

import (
	"testing"
)

func TestNegCompatible(t *testing.T) {
	mu1 := make(Mu)
	mu2 := make(Mu)
	mu3 := make(Mu)
	mu4 := make(Mu)
	mu5 := make(Mu)

	mu1[Variable("?x")] = Constant("a1")
	mu1[Variable("?y")] = Constant("a2")
	mu1[Variable("?p")] = Constant("a3")

	mu2[Variable("?x")] = Constant("a1")
	mu2[Variable("?y")] = Constant("a2")

	mu3[Variable("?x")] = Constant("a1")
	mu3[Variable("?y")] = Constant("a3")

	mu4[Variable("?x")] = Constant("a1")
	mu4[Variable("?z")] = Constant("a2")

	mu5[Variable("?q")] = Constant("a1")
	mu5[Variable("?z")] = Constant("a2")

	if mu1.negCompatible(&mu2) {
		t.Error("should not be negative compatible", mu1, mu2)
	}

	if !mu1.negCompatible(&mu3) {
		t.Error("should be negative compatible", mu1, mu3)
	}

	if !mu1.negCompatible(&mu4) {
		t.Error("should be negative compatible", mu1, mu4)
	}

	if !mu1.negCompatible(&mu5) {
		t.Error("should be negative compatible", mu1, mu5)
	}

}

func TestAtomMatches(t *testing.T) {

	a1 := newAtom(":a", ":link", ":b")
	a2 := newAtom(":a", ":link", ":a")

	bgp1 := newAtom("?x", ":link", "?y")
	bgp2 := newAtom("?x", ":link", "?x")
	bgp3 := newAtom("?x", ":notlink", "?y")

	if !bgp1.matches(&a1) {
		t.Error("should match:", bgp1, a1)
	}

	if !bgp1.matches(&a2) {
		t.Error("should match:", bgp1, a2)
	}

	if bgp2.matches(&a1) {
		t.Error("should not match:", bgp2, a1)
	}

	if !bgp2.matches(&a2) {
		t.Error("should match:", bgp2, a2)
	}

	if bgp3.matches(&a1) {
		t.Error("should not match:", bgp3, a1)
	}

	if bgp3.matches(&a2) {
		t.Error("should not match:", bgp3, a2)
	}
}

func TestDatabase(t *testing.T) {

	as, db := mkDatabase()

	if !db.isEdbRelation(":link") {
		t.Error("':link' should be a registered edb relation")
	}

	for _, a := range as {
		if !db.knows(a) {
			t.Error(a, false, true)
		}
	}

	omega := db.findMappingsFor(&Atom{Variable("?x"), Constant(":link"), Variable("?y"), false})

	if len(omega) != 4 {
		t.Error("len(omega):", len(omega), 4, len(omega))
	}

}
