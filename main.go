package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	dbUser  = flag.String("U", "root", "db user")
	dbHosts = flag.String("h", "localhost", "db host name or ip addr. multi hosts with split ,")
	// dbPassword = flag.String("p", "", "db password")
	dbPort      = flag.String("P", "4000", "db port")
	dbName      = flag.String("d", "test", "db name")
	reqNum      = flag.Int("n", 1000, "total request num")
	parallelNum = flag.Int("t", 20, "parallel number")
	debug       = flag.Bool("debug", false, "true if debug mode")
)

var db map[string]*sql.DB

// Test db struct
type Test struct {
	ID        int
	Code      string
	Text      string
	IsTest    bool
	CreatedAt time.Time
}

func main() {
	flag.Parse()
	hosts := strings.Split(*dbHosts, ",")
	db = make(map[string]*sql.DB, len(hosts))
	for _, h := range hosts {
		initDB(h)
	}
	var sema chan int = make(chan int, *parallelNum)
	begin := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *reqNum; i++ {
		hostIndex := i % len(hosts)
		host := hosts[hostIndex]
		wg.Add(1)
		go load(sema, &wg, host)
	}
	wg.Wait()
	end := time.Now()
	fmt.Println(end.Sub(begin))
	count := selectCount(hosts[0])
	fmt.Printf("insert num = %+v, select count = %v\n", *reqNum, count)
}

func load(sema chan int, wg *sync.WaitGroup, host string) {
	defer wg.Done()
	sema <- 1
	defer func() { <-sema }()
	code := insert(host, "")
	update(host, code)
	deleteOne(host, code)
	insert(host, code)
}

func initDB(dbHost string) {
	//dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable", *dbUser, *dbPassword, *dbName, dbHost, *dbPort)
	dbinfo := fmt.Sprintf("%s@tcp(%s:%s)/%s?parseTime=true", *dbUser, dbHost, *dbPort, *dbName)
	if *debug {
		log.Printf("dbinfo = %+v\n", dbinfo)
	}
	var err error
	db[dbHost], err = sql.Open("mysql", dbinfo)
	fatalIfErr(err)
	db[dbHost].SetMaxIdleConns(*parallelNum)
}

func connect(dbHost string) *sql.DB {
	return db[dbHost]
}

func insert(dbHost, code string) string {
	sql := "insert into test(code, text, is_test, created_at) values(?, ?, ?, ?)"
	db := connect(dbHost)
	if code == "" {
		code = generateUID()
	}
	result, err := db.Exec(sql, code, "test", true, time.Now())
	fatalIfErr(err)
	if *debug {
		rows, err := result.RowsAffected()
		fatalIfErr(err)
		lastID, err := result.LastInsertId()
		fatalIfErr(err)
		fmt.Printf("rows = %+v, last_insert_id = %+v\n", rows, lastID)
	}
	return code
}

func update(dbHost, code string) {
	sql := "update test set is_test = false where code = ?"
	db := connect(dbHost)
	result, err := db.Exec(sql, code)
	fatalIfErr(err)
	if *debug {
		rows, err := result.RowsAffected()
		fatalIfErr(err)
		lastID, err := result.LastInsertId()
		// fatalIfErr(err) => cockroachdbの場合は取れない
		fmt.Printf("rows = %+v, last_insert_id = %+v\n", rows, lastID)
	}
	t := selectOne(dbHost, code)
	if t.IsTest {
		log.Fatal("unexpected is_test = true when after update")
	}
}

func selectOne(dbHost, code string) *Test {
	sql := "select * from test where code = ?"
	db := connect(dbHost)

	rows, err := db.Query(sql, code)
	fatalIfErr(err)
	defer rows.Close()

	t := &Test{}
	rows.Next()
	if err := rows.Scan(&t.ID, &t.Code, &t.Text, &t.IsTest, &t.CreatedAt); err != nil {
		fatalIfErr(err)
	}
	return t
}

func deleteOne(dbHost, code string) {
	sql := "delete from test where code = ?"
	db := connect(dbHost)
	_, err := db.Exec(sql, code)
	fatalIfErr(err)
}

func selectList(dbHost string) []*Test {
	sql := "select * from test"
	db := connect(dbHost)

	rows, err := db.Query(sql)
	fatalIfErr(err)
	defer rows.Close()

	tests := make([]*Test, 0)
	for rows.Next() {
		t := &Test{}
		if err := rows.Scan(&t.Code, &t.Text, &t.IsTest, &t.CreatedAt); err != nil {
			fatalIfErr(err)
		}
		tests = append(tests, t)
	}
	return tests
}

func selectCount(dbHost string) int {
	sql := "select count(*) from test"
	db := connect(dbHost)

	rows, err := db.Query(sql)
	fatalIfErr(err)
	defer rows.Close()

	var count int
	rows.Next()
	if err := rows.Scan(&count); err != nil {
		fatalIfErr(err)
	}
	return count
}

func fatalIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

// https://qiita.com/shinofara/items/5353df4f4fbdaae3d959
func generateUID() string {
	buf := make([]byte, 10)

	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	str := fmt.Sprintf("%d%x", time.Now().Unix(), buf[0:10])
	return hex.EncodeToString([]byte(str))
}
