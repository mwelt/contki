package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

func genRngGraph(numNodes, numEdges, numEdgesExt int) (Database, Database, Database) {

	db1 := newDatabase()
	db1.registerEdbRel(":link")

	count := 0

	for count < numEdges {
		n1 := rand.Intn(numNodes)
		n2 := rand.Intn(numNodes)
		a := newAtom(":n"+strconv.Itoa(n1), ":link", ":n"+strconv.Itoa(n2))

		if !db1.knows(a) {
			db1.addAtom(a)
			count += 1
		}
	}

	db2 := db1.shallowCopy()

	count = 0

	for count < numEdgesExt {
		n1 := rand.Intn(numNodes)
		n2 := rand.Intn(numNodes)
		a := newAtom(":n"+strconv.Itoa(n1), ":link", ":n"+strconv.Itoa(n2))

		if !db1.knows(a) && !db2.knows(a) {
			db2.addAtom(a)
			count += 1
		}
	}

	db3 := db1.shallowCopy()

	count = 0

	for count < numEdgesExt {
		n1 := rand.Intn(numNodes)
		n2 := rand.Intn(numNodes)
		a := newAtom(":n"+strconv.Itoa(n1), ":link", ":n"+strconv.Itoa(n2))

		if !db1.knows(a) && !db3.knows(a) {
			db3.addAtom(a)
			count += 1
		}
	}

	return db1, db2, db3
}

func runNoInc(prog Program, db, append1, append2 Database) (Database, Database) {
	prog.evalSeminaiveAppend(&db, &append1)
	// startSN := time.Now()
	db.remove(&append1)
	db.clearIdb()
	prog.evalSeminaive(&db)
	// fmt.Println(uint64(time.Since(startSN) / time.Millisecond))
	db_after := db.deepCopy()
	prog.evalSeminaiveAppend(&db, &append2)
	return db_after, db
}

func runDRed(prog Program, db, append1, append2 Database) (Database, Database) {
	prog.evalSeminaiveAppend(&db, &append1)
	dRed(&db, &append1, &prog)
	db_after := db.deepCopy()
	prog.evalSeminaiveAppend(&db, &append2)
	return db_after, db
}

func runCommitRevert(prog Program, db, append1, append2 Database) (Database, Database) {
	db.commit()
	prog.evalSeminaiveAppend(&db, &append1)
	db.revert()
	db_after := db.deepCopy()
	prog.evalSeminaiveAppend(&db, &append2)
	return db_after, db
}

//// func testTransitiveClosure() {

//// 	S := 30
//// 	T := 500
//// 	inc := 5
//// 	N := 10

//// 	c1 := make(chan time.Duration)
//// 	c2 := make(chan time.Duration)
//// 	c3 := make(chan time.Duration)

//// 	for i := S; i <= T; i += inc {

//// 		s1 := int64(0)
//// 		s2 := int64(0)
//// 		s3 := int64(0)

//// 		nNodes := 10000
//// 		nEdges := 2000
//// 		nEdgesExt := i

//// 		for j := 0; j < N; j++ {

//// 			abox := genRngGraph(nNodes, nEdges+nEdgesExt)
//// 			aboxExt := abox[:nEdgesExt]
//// 			abox = abox[nEdgesExt:]

//// 			go func() {
//// 				c1 <- runNaiveDatalog(abox, aboxExt)
//// 			}()

//// 			go func() {
//// 				c2 <- runSemiNaiveDatalog(abox, aboxExt)
//// 			}()

//// 			go func() {
//// 				c3 <- runSemiNaiveDatalogExt(abox, aboxExt)
//// 			}()

//// 			s1 = int64(<-c1 / time.Millisecond)
//// 			s2 = int64(<-c2 / time.Millisecond)
//// 			s3 = int64(<-c3 / time.Millisecond)

//// 		}

//// 		fmt.Println(nNodes, ",", nEdges, ",", nEdgesExt, ",", s1/int64(N), ",", s2/int64(N), ",", s3/int64(N))
//// 	}

//// }

//// }}}

func benchmark() {

	// _, db := mkDatabase()
	prog := mkProgram()

	// append1 := db.shallowCopy()
	// append1.addAtom(newAtom(":c", ":link", ":c"))

	// db.remove(&append1)
	// prog.evalSeminaive(&db)

	for nEdgesExt := 1; nEdgesExt < 300; nEdgesExt += 2 {

		nNodes := 10000
		nEdges := 5000

		db, dbExt1, dbExt2 := genRngGraph(nNodes, nEdges, nEdgesExt)

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

		// if !dbAfterNoInc.equalTo(&dbAfterDRed) || !dbAfterDRed.equalTo(&dbAfterCR) {
		// 	panic("dabase instances were not equal, aborting.")
		// }

		fmt.Println(nNodes, nEdges, nEdgesExt,
			uint64(elapsedNoInc/time.Millisecond),
			uint64(elapsedDRed/time.Millisecond),
			uint64(elapsedCR/time.Millisecond))
	}

}
