package main

type Program []Rule

type DeltaProgram struct {
	rules  []Rule
	drules []DeltaRule
}

type DeltaRule struct {
	head, delta Atom
	body        []Atom
}

type Rule struct {
	head Atom
	body []Atom
}

func (prog *Program) register(db *Database) {
	for _, r := range *prog {
		r.register(db)
	}
}

func (r *Rule) register(db *Database) {

	if db.isEdbRelation(r.head.p.(Constant)) {
		panic("rule head relation already registered as EDB relation in database")
	}

	if !db.isIdbRelation(r.head.p.(Constant)) {
		db.registerIdbRel(r.head.p.(Constant))
	}

	for _, a := range r.body {
		if !db.isEdbRelation(a.p.(Constant)) && !db.isIdbRelation(a.p.(Constant)) {
			db.registerEdbRel(a.p.(Constant))
		}
	}

}

func (r *Rule) toDeltaRules(db *Database, idbOnly bool) []DeltaRule {
	drules := make([]DeltaRule, 0, len(r.body))

	for i, d := range r.body {
		if isConstant(d.p) && (!idbOnly || db.isIdbRelation(d.p.(Constant))) {
			dr := DeltaRule{head: r.head, delta: d, body: make([]Atom, 0, len(r.body)-1)}
			for j := 0; j < i; j++ {
				dr.body = append(dr.body, r.body[j])
			}
			for j := i + 1; j < len(r.body); j++ {
				dr.body = append(dr.body, r.body[j])
			}
			drules = append(drules, dr)
		}
	}

	return drules
}

func (prog *Program) toDeltaProgram(db *Database, idbOnly bool) DeltaProgram {
	dprog := DeltaProgram{rules: make([]Rule, 0), drules: make([]DeltaRule, 0)}
	for _, r := range *prog {
		drules := r.toDeltaRules(db, idbOnly)
		if len(drules) == 0 {
			dprog.rules = append(dprog.rules, r)
		} else {
			dprog.drules = append(dprog.drules, drules...)
		}
	}
	return dprog
}

// eval evaluates a DeltaRule w.r.t. to database instance and a delta
// database instance, and returns a multiset omega
func (r *DeltaRule) eval(db, delta *Database) Omega {

	omegas := make([]Omega, 0)

	omegas = append(omegas, delta.findMappingsFor(&(*r).delta))

	for _, b := range (*r).body {
		omegas = append(omegas, db.findMappingsFor(&b))
	}

	result := omegas[0]

	for i := 1; i < len(omegas); i++ {
		result = result.join(&omegas[i])
	}

	return result

}

// eval evaluates a DeltaRule w.r.t. to database instance and a delta
// database instance, and returns a multiset omega
func (r *Rule) eval(db *Database) Omega {

	omegas := make([]Omega, 0)

	for _, b := range (*r).body {
		omegas = append(omegas, db.findMappingsFor(&b))
	}

	result := omegas[0]

	for i := 1; i < len(omegas); i++ {
		result = result.join(&omegas[i])
	}

	return result

}

func (dprog *DeltaProgram) evalSeminaive_(db, delta *Database) Database {

	delta_ := db.shallowCopy()

	for _, r := range dprog.rules {
		omega := r.eval(db)
		for _, mu := range omega {
			groundHead := r.head.applyMapping(&mu)
			if !db.knows(groundHead) && !delta_.knows(groundHead) {
				delta_.addAtom(groundHead)
			}
		}
	}

	for _, r := range dprog.drules {
		omega := r.eval(db, delta)
		for _, mu := range omega {
			groundHead := r.head.applyMapping(&mu)
			if !db.knows(groundHead) && !delta_.knows(groundHead) {
				delta_.addAtom(groundHead)
			}
		}
	}

	return delta_
}

func (prog *Program) evalSeminaive(db *Database) {
	dprog := prog.toDeltaProgram(db, true)

	delta := dprog.evalSeminaive_(db, db)
	for !delta.empty() {
		db.append(&delta, false)
		delta = dprog.evalSeminaive_(db, &delta)
	}
}

func (prog *Program) evalNaive_(db *Database) Database {

	delta := db.shallowCopy()

	for _, r := range *prog {
		omega := r.eval(db)
		for _, mu := range omega {
			groundHead := r.head.applyMapping(&mu)
			if !db.knows(groundHead) && !delta.knows(groundHead) {
				delta.addAtom(groundHead)
			}
		}
	}

	return delta
}

func (prog *Program) evalNaive(db *Database) {
	delta := prog.evalNaive_(db)
	for !delta.empty() {
		db.append(&delta, false)
		delta = prog.evalNaive_(db)
	}
}

func (prog *Program) evalSeminaiveAppend(db, db_ *Database) {
	dprog := prog.toDeltaProgram(db, false)

	delta := dprog.evalSeminaive_(db, db_)
	db.append(db_, false)

	for !delta.empty() {
		db.append(&delta, false)
		delta = dprog.evalSeminaive_(db, &delta)
	}
}
