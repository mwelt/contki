package main

import "fmt"

func mkProgram() Program {

	r1 := Rule{
		head: newAtom("?x", ":reachable", "?y"),
		body: []Atom{newAtom("?x", ":link", "?y")}}

	r2 := Rule{
		head: newAtom("?x", ":reachable", "?y"),
		body: []Atom{
			newAtom("?x", ":link", "?z"),
			newAtom("?z", ":reachable", "?y")}}

	// semi positive datalog
	r3 := Rule{
		head: newAtom("?x", ":indirect", "?y"),
		body: []Atom{
			newNegAtom("?x", ":link", "?y"),
			newAtom("?x", ":reachable", "?y")}}

	return Program{r1, r2, r3}

}

func mkDatabase() ([]Atom, Database) {

	db := newDatabase()

	as := []Atom{
		newAtom(":a", ":link", ":b"),
		newAtom(":b", ":link", ":c"),
		newAtom(":c", ":link", ":c"),
		newAtom(":c", ":link", ":d"),
	}

	for _, a := range as {
		db.addAtom(a)
	}

	return as, db
}

func example() {

	_, db := mkDatabase()
	prog := mkProgram()

	fmt.Println("Start DB:")
	db.dump()

	prog.register(&db)

	// prog.evalNaive(&db)

	// fmt.Println("Finished Naive DB:")
	// db.dump()

	prog.evalSeminaive(&db)

	fmt.Println("Finished Seminaive DB:")
	db.dump()

	db.addAtom(newAtom(":a", ":link", ":c"))
	db.clearIdb()
	prog.evalSeminaive(&db)

	fmt.Println("Finished Seminaive DB:")
	db.dump()

	// del := db.shallowCopy()
	// del.addAtom(newAtom(":c", ":link", ":c"))

	// dRed(&db, &del, &prog)

	// fmt.Println("DB After DRed:")
	// db.dump()

	// db.commit()

	// db_ := db.shallowCopy()
	// db_.addAtom(newAtom(":c", ":link", ":c"))

	// prog.evalSeminaiveAppend(&db, &db_)

	// fmt.Println("DB After Append:")
	// db.dump()

	// db.revert()

	// fmt.Println("DB After Revert:")
	// db.dump()

}
