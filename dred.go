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
		// db.remove(&delta)
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

	// fmt.Println("DRed start:")
	// db.dump()
	// del.dump()

	// orig_del_size := del.size()
	// db_size := db.size()

	// overEstStart := time.Now()
	prog.evalOverEstimate(db, del)
	// overEstElapsed := time.Since(overEstStart)

	// fmt.Println("Overest:")
	// db.dump()
	// del.dump()

	// overest_size := del.size()

	// removeRelsStart := time.Now()
	// removeRels(&(*db).edb, &(*del).edb)
	db.remove(del)
	// removeRelsElapsed := time.Since(removeRelsStart)

	// fmt.Println("Removed Overest:")
	// db.dump()
	// del.dump()

	// del_size := del.size()

	// altDeriveStart := time.Now()
	prog.evalAltDerive(db, del)
	// altDeriveElapsed := time.Since(altDeriveStart)

	// fmt.Println("Rederived:")
	// db.dump()
	// del.dump()

	// fmt.Println(
	// 	orig_del_size,
	// 	db_size,
	// 	overest_size,
	// 	del_size,
	// 	uint64(overEstElapsed/time.Millisecond),
	// 	uint64(removeRelsElapsed/time.Millisecond),
	// 	uint64(altDeriveElapsed/time.Millisecond))

}
