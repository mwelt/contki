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

	return Program{r1, r2}

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
	prog.evalSeminaive(&db)

	fmt.Println("Finished Seminaive DB:")
	db.dump()

	del := db.shallowCopy()
	del.addAtom(newAtom(":c", ":link", ":c"))

	dRed(&db, &del, &prog)

	fmt.Println("DB After DRed:")
	db.dump()

	db.commit()

	db_ := db.shallowCopy()
	db_.addAtom(newAtom(":c", ":link", ":c"))

	prog.evalSeminaiveAppend(&db, &db_)

	fmt.Println("DB After Append:")
	db.dump()

	db.revert()

	fmt.Println("DB After Revert:")
	db.dump()

}
