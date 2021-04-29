package main

import (
	"testing"
)

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

	omega := db.findMappingsFor(&Atom{Variable("?x"), Constant(":link"), Variable("?y")})

	if len(omega) != 4 {
		t.Error("len(omega):", len(omega), 4, len(omega))
	}

}
