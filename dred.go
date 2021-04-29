package main

func (dprog *DeltaProgram) evalOverEstimate_(db, del *Database) Database {

	delta_ := db.shallowCopy()

	for _, r := range dprog.drules {
		omega := r.eval(db, del)
		for _, mu := range omega {
			groundHead := r.head.applyMapping(&mu)
			if !del.knows(groundHead) && !delta_.knows(groundHead) {
				delta_.addAtom(groundHead)
			}
		}
	}

	return delta_
}

func (prog *Program) evalOverEstimate(db, del *Database) {
	dprog := prog.toDeltaProgram(db, false)

	delta := dprog.evalOverEstimate_(db, del)
	for !delta.empty() {
		del.append(&delta, false)
		db.remove(&delta)
		delta = dprog.evalOverEstimate_(db, del)
	}
}

func (prog *Program) toAltDeriveDeltaProgram() DeltaProgram {
	dprog := DeltaProgram{rules: make([]Rule, 0), drules: make([]DeltaRule, 0)}
	for _, r := range *prog {
		dr := DeltaRule{head: r.head, delta: r.head, body: r.body}
		dprog.drules = append(dprog.drules, dr)
	}
	return dprog
}

func (dprog *DeltaProgram) evalAltDerive_(db, del *Database) Database {

	delta_ := db.shallowCopy()

	for _, r := range dprog.drules {
		omega := r.eval(db, del)
		for _, mu := range omega {
			groundHead := r.head.applyMapping(&mu)
			if !db.knows(groundHead) && !delta_.knows(groundHead) {
				delta_.addAtom(groundHead)
			}
		}
	}

	return delta_
}

func (prog *Program) evalAltDerive(db, del *Database) {
	dprog := prog.toAltDeriveDeltaProgram()

	delta := dprog.evalAltDerive_(db, del)
	for !delta.empty() {
		db.append(&delta, false)
		delta = dprog.evalAltDerive_(db, del)
	}
}

func dRed(db, del *Database, prog *Program) {

	prog.evalOverEstimate(db, del)
	removeRels(&(*db).edb, &(*del).edb)

	prog.evalAltDerive(db, del)

}
