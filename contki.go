package main

func TestDRed() {

	_, db := mkDatabase()

	prog := mkProgram()

	prog.register(&db)

	prog.evalSeminaive(&db)

	del := newDatabase()
	prog.register(&del)

	as := []Atom{
		newAtom(":c", ":link", ":d"),
	}

	for _, a := range as {
		del.addAtom(a)
	}

	dRed(&db, &del, &prog)

}

func main() {
	// example()
	benchmark2()
	// TestDRed()
}
