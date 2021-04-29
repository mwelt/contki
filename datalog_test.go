package main

import "testing"

func TestRegisterProgram(t *testing.T) {

	_, db := mkDatabase()

	prog := mkProgram()

	prog.register(&db)

	if !db.isEdbRelation(Constant(":link")) {
		t.Error("':link' was not registered as edb relation")
	}

	if !db.isIdbRelation(Constant(":reachable")) {
		t.Error("':reachable' was not registered as idb relation")
	}

}

func TestMkDeltaRules(t *testing.T) {

	_, db := mkDatabase()
	prog := mkProgram()

	prog.register(&db)

	drules1 := prog[0].toDeltaRules(&db, true)

	if len(drules1) > 0 {
		t.Error("created delta rules for rule with no idb predicates", drules1)
	}

	drules2 := prog[1].toDeltaRules(&db, true)

	if len(drules2) != 1 {
		t.Error("expected exactly one delta rule", drules2, prog[1])
	}

	drule1 := drules2[0]
	if prog[1].head != drule1.head {
		t.Error("wrong head in delta rule", drule1)
	}

	drule1Body1 := drules2[0].body[0]
	if prog[1].body[0] != drule1Body1 {
		t.Error("not equal", prog[1].body[0], drule1Body1)
	}

	if prog[1].body[1] != drule1.delta {
		t.Error("wrong delta", drule1.delta, prog[1].body[1])
	}

}

func TestProgMkDeltaRules(t *testing.T) {

	_, db := mkDatabase()
	prog := mkProgram()

	prog.register(&db)

	dprog := prog.toDeltaProgram(&db, false)

	if len(dprog.rules) > 0 {
		t.Error("should not have generated normal rules", dprog.rules)
	}

}
