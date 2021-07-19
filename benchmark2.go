package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

func mkProgram2() Program {

	r1 := Rule{
		head: newAtom("?x", ":can", ":fly"),
		body: []Atom{newAtom("?x", ":has", ":Wings")}}

	return Program{r1}

}

func coinFlip() bool {
	return rand.Float32() < 0.5
}

func genRngAnimals(numAnimals, numExt int) (Database, int, Database, int, Database, int) {

	db1 := newDatabase()
	db1.registerEdbRel(":a")
	db1.registerEdbRel(":has")

	count := 0

	cflip1 := 0
	cflip2 := 0
	cflip3 := 0

	for count < numAnimals {
		db1.addAtom(newAtom(":n"+strconv.Itoa(count), ":a", ":Animal"))

		if coinFlip() {
			db1.addAtom(newAtom(":n"+strconv.Itoa(count), ":has", ":Wings"))
			cflip1 += 1
		}
		count += 1
	}

	db2 := db1.shallowCopy()

	for count < numAnimals+numExt {
		db2.addAtom(newAtom(":n"+strconv.Itoa(count), ":a", ":Animal"))

		if coinFlip() {
			db2.addAtom(newAtom(":n"+strconv.Itoa(count), ":has", ":Wings"))
			cflip1 += 2
		}

		count += 1
	}

	db3 := db1.shallowCopy()

	for count < numAnimals+2*numExt {
		db3.addAtom(newAtom(":n"+strconv.Itoa(count), ":a", ":Animal"))

		if coinFlip() {
			db3.addAtom(newAtom(":n"+strconv.Itoa(count), ":has", ":Wings"))
			cflip1 += 3
		}

		count += 1
	}

	return db1, cflip1, db2, cflip2, db3, cflip3
}

func benchmark2() {

	// _, db := mkDatabase()
	prog := mkProgram2()

	// append1 := db.shallowCopy()
	// append1.addAtom(newAtom(":c", ":link", ":c"))

	// db.remove(&append1)
	// prog.evalSeminaive(&db)

	for nExt := 100; nExt < 1000; nExt += 2 {

		nAnimals := 5000

		db, cflip1, dbExt1, cflip2, dbExt2, cflip3 := genRngAnimals(nAnimals, nExt)

		prog.register(&db)
		prog.register(&dbExt1)
		prog.register(&dbExt2)

		prog.evalSeminaive(&db)

		startNoInc := time.Now()
		intermedDbNoInc, dbAfterNoInc := runNoInc(prog, db.deepCopy(), dbExt1.deepCopy(), dbExt2.deepCopy())
		elapsedNoInc := time.Since(startNoInc)
		startDRed := time.Now()
		intermedDbDRed, dbAfterDRed := runDRed(prog, db.deepCopy(), dbExt1.deepCopy(), dbExt2.deepCopy())
		elapsedDRed := time.Since(startDRed)
		startCR := time.Now()
		intermedDbCR, dbAfterCR := runCommitRevert(prog, db.deepCopy(), dbExt1.deepCopy(), dbExt2.deepCopy())
		elapsedCR := time.Since(startCR)

		if !intermedDbNoInc.equalTo(&intermedDbDRed) {
			panic("iterm. NoInc != DRed")
		}

		if !dbAfterNoInc.equalTo(&dbAfterDRed) {
			panic("final NoInc != DRed")
		}

		if !intermedDbNoInc.equalTo(&intermedDbCR) {
			panic("iterm. NoInc != CR")
		}

		if !dbAfterNoInc.equalTo(&dbAfterCR) {
			panic("final NoInc != CR")
		}

		if !intermedDbDRed.equalTo(&intermedDbCR) {
			panic("iterm. DRed != CR")
		}

		if !dbAfterDRed.equalTo(&dbAfterCR) {
			panic("final DRed != CR")
		}

		fmt.Println(nExt,
			uint64(elapsedNoInc/time.Millisecond),
			uint64(elapsedDRed/time.Millisecond),
			uint64(elapsedCR/time.Millisecond),
			cflip1,
			cflip2,
			cflip3)
	}

}
